package com.wajam.nrv.service

import scala.actors.Actor
import com.yammer.metrics.scala.Instrumented
import com.wajam.commons.Logging
import com.wajam.nrv.data.{Message, MessageType, InMessage, OutMessage}
import com.wajam.nrv.{UnavailableException, TimeoutException}
import java.util.concurrent.atomic.AtomicBoolean
import com.twitter.util.RingBuffer
import com.google.common.cache.{RemovalNotification, RemovalListener, CacheBuilder}
import java.util.concurrent.TimeUnit
import java.text.SimpleDateFormat
import com.wajam.nrv.utils.Scheduler

/**
 * Handle incoming messages and find matching outgoing messages, having same
 * rendez-vous number.
 */
class Switchboard(val name: String = "", val numExecutor: Int = 100, val maxTaskExecutorQueueSize: Int = 50,
                  val banExpirationDuration: Long = 30000L, val banThreshold: Double = 1.0)
  extends Actor with MessageHandler with Logging with Instrumented {

  require(numExecutor > 0)
  require(maxTaskExecutorQueueSize > 0)

  import Switchboard.SwitchboardCallback

  private val TIMEOUT_CHECK_IN_MS = 100
  private val rendezvous = collection.mutable.HashMap[Int, SentMessageContext]()
  private var id = 0
  private val started = new AtomicBoolean(false)
  private val executors = Array.fill(numExecutor) {
    new SwitchboardExecutor
  }

  // instrumentation
  private def metricName = if (name.isEmpty) null else name

  lazy private val received = metrics.meter("received", "received", metricName)
  lazy private val sent = metrics.meter("sent", "sent", metricName)
  lazy private val timeout = metrics.meter("timeout", "timeout", metricName)
  private val pending = metrics.gauge("pending", metricName) {
    rendezvous.size
  }

  lazy private val rejectedMessages = metrics.meter("rejected", "messages", metricName)
  lazy private val unexpectedResponses = metrics.meter("unexpected", "responses", metricName)
  private val executorQueueSize = metrics.gauge("executors-queue-size", metricName) {
    executors.foldLeft[Int](0)((sum: Int, actor: SwitchboardExecutor) => {
      sum + actor.queueSize
    })
  }

  lazy private val bannedMessages = metrics.meter("banned", "messages", metricName)
  lazy private val bannedDurationTimer = metrics.timer("banned-token-time", metricName)
  private val bannedListSize = metrics.gauge("banned-list-size", metricName) {
    executors.foldLeft(0L)((sum, executor: SwitchboardExecutor) => {
      sum + executor.bannedListSize
    })
  }

  // Only created once the switchboard is started. This is a workaround to not create a thread for every Switchboard
  // created by unit tests as most of these instances are not even started.
  private var checkTimeoutScheduler: Option[Scheduler] = None

  override def start(): Actor = {
    if (started.compareAndSet(false, true)) {
      super.start()
      checkTimeoutScheduler = Some(new Scheduler(this, CheckTimeout, TIMEOUT_CHECK_IN_MS, TIMEOUT_CHECK_IN_MS,
        blockingMessage = true, name = Some("Switchboard.CheckTimeout")))

      for (e <- executors) {
        e.start()
      }
    }

    this
  }

  def stop() {
    if (started.compareAndSet(true, false)) {
      checkTimeoutScheduler.foreach(_.cancel())
    }
  }

  override def handleOutgoing(action: Action, outMessage: OutMessage) {
    this.handleOutgoing(action, outMessage, () => {})
  }

  override def handleOutgoing(action: Action, outMessage: OutMessage, next: () => Unit) {
    outMessage.function match {
      case MessageType.FUNCTION_CALL =>
        this !(new SentMessageContext(action, outMessage), SwitchboardCallback(next))

      case _ =>
        next()
    }
  }

  override def handleIncoming(action: Action, inMessage: InMessage) {
    this.handleIncoming(action, inMessage, () => {})
  }

  override def handleIncoming(action: Action, inMessage: InMessage, next: () => Unit) {
    inMessage.matchingOutMessage match {
      // no matching out message, we need to find matching message
      case None =>
        this !(new ReceivedMessageContext(action, inMessage), SwitchboardCallback(next))

      case Some(outMessage) =>
        next()
    }
  }

  protected[nrv] def checkTimeout() {
    checkTimeoutScheduler.foreach(_.forceSchedule())
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
        case (sending: SentMessageContext, SwitchboardCallback(next)) => {
          try {
            sent.mark()

            sending.outMessage.rendezvousId = nextId
            rendezvous += (sending.outMessage.rendezvousId -> sending)

            execute(sending.action, sending.outMessage, next)
          } catch {
            case e: Exception => error("Error in switchboard processing SentMessageContext: {}", e)
          }
        }
        case (receiving: ReceivedMessageContext, SwitchboardCallback(next)) => {
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

  private def execute(action: Action, message: Message, next: (() => Unit)) {
    if (message.token >= 0) {
      val executor = executors((message.token % numExecutor).toInt)
      executor.execute(action, message, next)
    } else {
      throw new RuntimeException("Invalid token for message: " + message)
    }
  }

  private class SentMessageContext(val action: Action, val outMessage: OutMessage) {
    val enventualTimeoutException = new TimeoutException("Timeout receiving a reply in time")
  }

  private class ReceivedMessageContext(val action: Action, val inMessage: InMessage)

  private object CheckTimeout

  private case class TokenBan(token: Long, createTime: Long = getTime(),
                              var lastAttemptTime: Long = getTime(), var attemptAfterBan: Int = 0) {
    override def toString = {
      "<tk=%d, attempts=%d, banned=%s, lastAttempt=%s>".format(
        token, attemptAfterBan, formatTimestamp(createTime), formatTimestamp(lastAttemptTime))
    }

    private def formatTimestamp(timestamp: Long) = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(timestamp)
  }

  private class SwitchboardExecutor extends Actor with Logging {

    private val bannedTokenRemovalListener = new RemovalListener[java.lang.Long, TokenBan] {
      def onRemoval(notification: RemovalNotification[java.lang.Long, TokenBan]) {
        val ban = notification.getValue
        val banDuration = ban.lastAttemptTime - ban.createTime + banExpirationDuration
        bannedDurationTimer.update(banDuration, TimeUnit.MILLISECONDS)
        info("Ban lifted {}", notification.getValue)
      }
    }

    private val recentTokens = new RingBuffer[Long](maxTaskExecutorQueueSize)
    private val bannedTokens = CacheBuilder.newBuilder().expireAfterAccess(
      banExpirationDuration, TimeUnit.MILLISECONDS).removalListener(bannedTokenRemovalListener).build[java.lang.Long, TokenBan]

    def queueSize = mailboxSize

    def bannedListSize = bannedTokens.size()

    def execute(action: Action, message: Message, next: (() => Unit)) {
      if (!rejectMessageWhenOverloadedOrBanned(action, message)) {
        recentTokens += message.token
        this ! SwitchboardCallback(next)
      }
    }

    def rejectMessageWhenOverloadedOrBanned(action: Action, message: Message): Boolean = {
      // Reject only incoming message that are function calls (i.e. request from other nodes/clients)
      message match {
        case inMessage: InMessage if inMessage.function == MessageType.FUNCTION_CALL => {
          Option(bannedTokens.getIfPresent(long2Long(message.token))) match {
            case Some(ban) => {
              // There is a ban on this token, reject the message with "Too many requests" error
              bannedMessages.mark()
              debug("Reject banned message {} (queueSize={})", ban, queueSize)

              ban.attemptAfterBan += 1
              ban.lastAttemptTime = getTime()
              replyError(action, message, 429, new UnavailableException)
              true
            }
            case _ if queueSize >= maxTaskExecutorQueueSize => {
              // Executor queue is overloaded, reject the message and create ban if needed
              rejectedMessages.mark()
              debug("Executor queue overflow. Rejecting message (queueSize={}, tk={})", queueSize, message.token)

              // Ban queued tokens wich are exceeding the ban ratio
              for ((token, tokens) <- recentTokens.groupBy(k => k)) {
                if (tokens.size > maxTaskExecutorQueueSize * banThreshold) {
                  info("Executor queue overflow. Banning token {} because queue ratio {} is exceeding threshold {} (queueSize={})",
                    token, tokens.size / maxTaskExecutorQueueSize.toDouble, banThreshold, queueSize)
                  bannedTokens.put(long2Long(token), TokenBan(token))
                }
              }

              replyError(action, message, 503, new UnavailableException)
              true
            }
            case _ => false
          }
        }
        case _ => false
      }
    }

    private def replyError(action: Action, message: Message, code: Int, error: Exception) {
      val errorMessage = new OutMessage()
      action.generateResponseMessage(message, errorMessage)
      errorMessage.error = Some(error)
      errorMessage.code = code
      action.callOutgoingHandlers(errorMessage)
    }

    def act() {
      loop {
        react {
          case SwitchboardCallback(next) =>
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

object Switchboard {

  private case class SwitchboardCallback(next: () => Unit)

}
