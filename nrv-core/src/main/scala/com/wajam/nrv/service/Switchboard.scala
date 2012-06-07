package com.wajam.nrv.service

import actors.Actor
import collection.mutable.Map
import com.yammer.metrics.scala.Instrumented
import com.wajam.nrv.data.{MessageType, InMessage, OutMessage}
import java.util.{TimerTask, Timer}
import com.wajam.nrv.{TimeoutException, Logging}

/**
 * Handle incoming messages and find matching outgoing messages, having same
 * rendez-vous number.
 */
class Switchboard extends Actor with MessageHandler with Logging with Instrumented {
  val TIMEOUT_CHECK = 10

  private var rendezvous = Map[Int, RendezVous]()
  private var id = 0
  private var timer = new Timer()

  // instrumentation
  private val received = metrics.meter("received", "received")
  private val pending = metrics.gauge("pending") {
    rendezvous.size
  }

  /**
   * Returns current time, overridden by unit tests
   */
  protected[nrv] var getTime: (() => Long) = () => {
    System.currentTimeMillis()
  }

  private class RendezVous(val action: Action, val outMessage: OutMessage)

  private object CheckTimeout

  override def start(): Actor = {
    val actor = super.start()

    timer.scheduleAtFixedRate(new TimerTask {
      def run() {
        checkTimeout()
      }
    }, 0, TIMEOUT_CHECK)

    actor
  }

  protected[nrv] def checkTimeout() {
    Switchboard.this !? CheckTimeout
  }

  def stop() {
    this.timer.cancel()
  }

  override def handleOutgoing(action: Action, outMessage: OutMessage) {
    this.handleOutgoing(action, outMessage, Unit => {})
  }

  override def handleOutgoing(action: Action, outMessage: OutMessage, next: Unit => Unit) {
    outMessage.function match {
      case MessageType.FUNCTION_CALL =>
        this !(new RendezVous(action, outMessage), next)

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
        this !(inMessage, next)

      case Some(outMessage) =>
        next()
    }
  }

  def act() {
    while (true) {
      try {
        receive {

          case CheckTimeout =>
            var toRemove = List[Int]()

            for ((id, rdv) <- this.rendezvous) {
              val elaps = this.getTime() - rdv.outMessage.sentTime
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
              this.rendezvous -= id
            }

            sender ! true

          case (rdv: RendezVous, next: (Unit => Unit)) =>
            this.received.mark()

            this.id += 1
            rdv.outMessage.rendezvousId = this.id
            this.rendezvous += (this.id -> rdv)

            if (this.id > Int.MaxValue)
              this.id = 0

            next()

          case (inMessage: InMessage, next: (Unit => Unit)) =>
            this.received.mark()

            // check for rendez-vous
            if (inMessage.function == MessageType.FUNCTION_RESPONSE) {
              val optRdv = this.rendezvous.remove(inMessage.rendezvousId)
              optRdv match {
                case Some(rdv) =>
                  inMessage.matchingOutMessage = Some(rdv.outMessage)

                case None =>
                  warn("Received a incoming message with a rendez-vous, but with no matching outgoing message: {}", inMessage)

              }
            }

            next()

        }
      } catch {
        case e: Exception => error("Catched an exception in switchboard! Should never happen!", e)
      }
    }
  }
}
