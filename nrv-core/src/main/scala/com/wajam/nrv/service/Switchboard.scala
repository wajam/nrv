package com.wajam.nrv.service

import actors.Actor
import com.yammer.metrics.scala.Instrumented
import com.wajam.nrv.data.{Message, MessageType, InMessage, OutMessage}
import java.util.TimerTask
import com.wajam.nrv.{TimeoutException, Logging}
import java.util
import util.concurrent.atomic.AtomicBoolean

/**
 * Handle incoming messages and find matching outgoing messages, having same
 * rendez-vous number.
 */
class Switchboard(val numExecutor: Int = 100) extends Actor with MessageHandler with Logging with Instrumented {
  private val TIMEOUT_CHECK = 10
  private val random = new util.Random
  private val rendezvous = collection.mutable.HashMap[Int, Sending]()
  private val timer = new util.Timer
  private var id = 0
  private val executors = Array.fill(numExecutor) {new SwitchboardExecutor}

  @volatile
  private var started = new AtomicBoolean(false)

  // instrumentation
  private val received = metrics.meter("received", "received")
  private val pending = metrics.gauge("pending") {
    rendezvous.size
  }

  override def start(): Actor = {
    if (started.compareAndSet(false, true)) {
      super.start()
      timer.scheduleAtFixedRate(new TimerTask {
        def run() {
          checkTimeout()
        }
      }, 0, TIMEOUT_CHECK)
      for (e <- executors) {
        e.start()}
    }

    this
  }

  def stop() {
    if (started.compareAndSet(true, false)) {
      this.timer.cancel()
    }
  }

  override def handleOutgoing(action: Action, outMessage: OutMessage) {
    this.handleOutgoing(action, outMessage, Unit => {})
  }

  override def handleOutgoing(action: Action, outMessage: OutMessage, next: Unit => Unit) {
    outMessage.function match {
      case MessageType.FUNCTION_CALL =>
        this !(new Sending(action, outMessage), next)

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
        this ! (new Receiving(action, inMessage), next)

      case Some(outMessage) =>
        next()
    }
  }

  protected[nrv] def checkTimeout() {
    this !? CheckTimeout
  }

  /**
   * Returns current time, overridden by unit tests
   */
  protected[nrv] var getTime: (() => Long) = () => {
    System.currentTimeMillis()
  }

  private def nextId = {
    id += 1
    if(id == Int.MaxValue) {
      id = 0
    }
    id
  }

  def act() {
    while (true) {
      receive {
        case CheckTimeout =>
          var toRemove = List[Int]()

          for ((id, rdv) <- rendezvous) {
            val elaps = getTime() - rdv.outMessage.sentTime
            if (elaps >= rdv.outMessage.timeoutTime) {
              var exceptionMessage = new InMessage
              exceptionMessage.matchingOutMessage = Some(rdv.outMessage)
              exceptionMessage.error = Some(new TimeoutException("Didn't receive a reply within time"))
              rdv.action.generateResponseMessage(rdv.outMessage, exceptionMessage)
              rdv.action.callIncomingHandlers(exceptionMessage)

              toRemove :+= id
            }
          }

          for (id <- toRemove) {
            rendezvous -= id
          }

          sender ! true

        case (sending: Sending, next: (Unit => Unit)) =>
          received.mark()

          sending.outMessage.rendezvousId = nextId
          rendezvous += (sending.outMessage.rendezvousId -> sending)

          execute(sending.action, sending.outMessage, next)

        case (receiving: Receiving, next: (Unit => Unit)) =>
          // check for rendez-vous
          if (receiving.inMessage.function == MessageType.FUNCTION_RESPONSE) {
            val optRdv = rendezvous.remove(receiving.inMessage.rendezvousId)
            optRdv match {
              case Some(rdv) =>
                receiving.inMessage.matchingOutMessage = Some(rdv.outMessage)

              case None =>
                warn("Received a incoming message with a rendez-vous, but with no matching outgoing message: {}",
                  receiving.inMessage)

            }
          }

          execute(receiving.action, receiving.inMessage, next)
      }
    }
  }

  private def execute(action: Action, message: Message, next: (Unit => Unit)) {
    val index = if (action != null && action.resolver != null) {
      action.resolver.extractToken(action, message)
    } else {
      math.abs(random.nextInt())
    }
    executors((index % numExecutor).toInt) ! next
  }

  private class Sending(val action: Action, val outMessage: OutMessage)
  private class Receiving(val action: Action, val inMessage: InMessage)

  private object CheckTimeout

  private class SwitchboardExecutor extends Actor with Logging {

    def act() {
      loop {
        try {
          react {
            case next: (Unit => Unit) => next()
          }
        } catch {
          case e: Exception => error("Catched an exception in switchboard! Should never happen!", e)
        }
      }
    }
  }
}
