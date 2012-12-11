package com.wajam.nrv.service

import actors.Actor
import com.yammer.metrics.scala.Instrumented
import com.wajam.nrv.data.{Message, MessageType, InMessage, OutMessage}
import com.wajam.nrv.{UnavailableException, TimeoutException, Logging}
import java.util.concurrent.atomic.AtomicBoolean
import com.wajam.nrv.utils.Scheduler

/**
 * Handle incoming messages and find matching outgoing messages, having same
 * rendez-vous number.
 */
class Switchboard(val name: String = "", val numExecutor: Int = 100, val maxTaskExecutorQueueSize: Int = 50)
  extends Actor with MessageHandler with Logging with Instrumented {

  require(numExecutor > 0)
  require(maxTaskExecutorQueueSize > 0)

  private val TIMEOUT_CHECK_IN_MS = 100
  private val rendezvous = collection.mutable.HashMap[Int, SentMessageContext]()
  private var id = 0
  private val started = new AtomicBoolean(false)
  private val executors = Array.fill(numExecutor) {
    new SwitchboardExecutor
  }

  // instrumentation
  private def metricName = if (name.isEmpty) null else name
  private val received = metrics.meter("received", "received", metricName)
  private val sent = metrics.meter("sent", "sent", metricName)
  private val timeout = metrics.meter("timeout", "timeout", metricName)
  private val pending = metrics.gauge("pending", metricName) {
    rendezvous.size
  }

  private val rejectedMessages = metrics.meter("rejected", "messages", metricName)
  private val unexpectedResponses = metrics.meter("unexpected", "responses", metricName)
  private val executorQueueSize = metrics.gauge("executors-queue-size", metricName) {
    executors.foldLeft[Int](0)((sum: Int, actor: SwitchboardExecutor) => {
      sum + actor.queueSize
    })
  }

  // scheduled
  val schdlr = new Scheduler(this, CheckTimeout, TIMEOUT_CHECK_IN_MS, TIMEOUT_CHECK_IN_MS, blockingMessage = true)

  override def start(): Actor = {
    if (started.compareAndSet(false, true)) {
      super.start()
      schdlr.start()

      for (e <- executors) {
        e.start()
      }
    }

    this
  }

  def stop() {
    if (started.compareAndSet(true, false)) {
      this.schdlr.cancel()
    }
  }

  override def handleOutgoing(action: Action, outMessage: OutMessage) {
    this.handleOutgoing(action, outMessage, Unit => {})
  }

  override def handleOutgoing(action: Action, outMessage: OutMessage, next: Unit => Unit) {
    outMessage.function match {
      case MessageType.FUNCTION_CALL =>
        this !(new SentMessageContext(action, outMessage), next)

      case _ =>
        next()
    }
  }

  override def handleIncoming(action: Action, inMessage: InMessage) {
    this.handleIncoming(action, inMessage, Unit => {})
  }

  override def handleIncoming(action: Action, inMessage: InMessage, next: Unit => Unit) {
    inMessage.matchingOutMessage match {
      // no matching out message, we need to find matching message
      case None =>
        this !(new ReceivedMessageContext(action, inMessage), next)

      case Some(outMessage) =>
        next()
    }
  }

  protected[nrv] def checkTimeout() {
    this.schdlr.forceSchedule()
  }

  /**
   * Returns current time, overridden by unit tests
   */
  protected[nrv] var getTime: (() => Long) = () => {
    System.currentTimeMillis()
  }

  private def nextId = {
    id += 1
    if (id == Int.MaxValue) {
      id = 0
    }
    id
  }

  def act() {
    while (true) {
      receive {
        case CheckTimeout => {
          try {
            var toRemove = List[Int]()

            for ((id, rdv) <- rendezvous) {
              val elaps = getTime() - rdv.outMessage.sentTime
              if (elaps >= rdv.outMessage.responseTimeout) {
                timeout.mark()

                var exceptionMessage = new InMessage
                exceptionMessage.matchingOutMessage = Some(rdv.outMessage)
                rdv.enventualTimeoutException.elapsed = Some(elaps)
                exceptionMessage.error = Some(rdv.enventualTimeoutException)
                rdv.action.generateResponseMessage(rdv.outMessage, exceptionMessage)
                rdv.action.callIncomingHandlers(exceptionMessage)

                toRemove :+= id
              }
            }

            for (id <- toRemove) {
              rendezvous -= id
            }

            sender ! true
          } catch {
            case e: Exception => error("Error in switchboard processing CheckTimeout: {}", e)
          }
        }
        case (sending: SentMessageContext, next: (Unit => Unit)) => {
          try {
            sent.mark()

            sending.outMessage.rendezvousId = nextId
            rendezvous += (sending.outMessage.rendezvousId -> sending)

            execute(sending.action, sending.outMessage, next)
          } catch {
            case e: Exception => error("Error in switchboard processing SentMessageContext: {}", e)
          }
        }
        case (receiving: ReceivedMessageContext, next: (Unit => Unit)) => {
          try {
            received.mark()

            // check for rendez-vous
            if (receiving.inMessage.function == MessageType.FUNCTION_RESPONSE) {
              val optRdv = rendezvous.remove(receiving.inMessage.rendezvousId)
              optRdv match {
                case Some(rdv) =>
                  receiving.inMessage.matchingOutMessage = Some(rdv.outMessage)

                case None =>
                  unexpectedResponses.mark()
                  debug("Received a incoming message with a rendez-vous, but with no matching outgoing message: {}",
                    receiving.inMessage)

              }
            }
            execute(receiving.action, receiving.inMessage, next)
          } catch {
            case e: Exception => error("Error in switchboard processing ReceivedMessageContext: {}", e)
          }
        }
      }
    }
  }

  private def execute(action: Action, message: Message, next: (Unit => Unit)) {
    if (message.token >= 0) {
      val executor = executors((message.token % numExecutor).toInt)
      if(!rejectMessageWhenOverloaded(action, message, executor)) {
        executor ! next
      }
    } else {
      throw new RuntimeException("Invalid token for message: " + message)
    }
  }

  private def rejectMessageWhenOverloaded(action: Action, message: Message, executor: SwitchboardExecutor): Boolean = {
    // Reject only incoming message that are function calls (i.e. request from other nodes/clients)
    message match {
      case inMessage: InMessage => {
        val rejected = (inMessage.function == MessageType.FUNCTION_CALL
          && executor.queueSize >= maxTaskExecutorQueueSize)
        if (rejected) {
          rejectedMessages.mark()
          val rejectedMessage = new OutMessage()
          action.generateResponseMessage(message, rejectedMessage)
          rejectedMessage.error = Some(new UnavailableException)
          rejectedMessage.code = 503

          action.callOutgoingHandlers(rejectedMessage)
        }
        rejected
      }
      case _ => false
    }
  }

  private class SentMessageContext(val action: Action, val outMessage: OutMessage) {
    val enventualTimeoutException = new TimeoutException("Timeout receiving a reply in time")
  }

  private class ReceivedMessageContext(val action: Action, val inMessage: InMessage)

  private object CheckTimeout

  private class SwitchboardExecutor extends Actor with Logging {

    def queueSize = mailboxSize

    def act() {
      loop {
        react {
          case next: (Unit => Unit) =>
          try {
            next()
          } catch {
            case e: Exception => error("Got an error in SwitchboardExecutor: ", e)
          }
        }
      }
    }
  }

}
