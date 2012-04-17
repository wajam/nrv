package com.wajam.nrv.service

import actors.Actor
import collection.mutable.Map
import com.wajam.nrv.Logging
import com.yammer.metrics.scala.Instrumented
import com.wajam.nrv.data.{Message, MessageType, InMessage, OutMessage}

/**
 * Handle incoming messages and find matching outgoing messages, having same
 * rendez-vous number.
 */
class Switchboard extends Actor with MessageHandler with Logging with Instrumented {
  private var messages = Map[Int, OutMessage]()
  private var id = 0

  private val rdvGauge = metrics.gauge("pending-rendezvous") {
    messages.size
  }

  // TODO: timeouts (w/cleanup)

  override def handleOutgoing(action: Action, outMessage: Message) {
    this.handleOutgoing(action, outMessage, Unit=>{})
  }

  override def handleOutgoing(action: Action, outMessage: Message, next: Unit => Unit) {
    this ! (outMessage, next)
  }

  override def handleIncoming(action: Action, outMessage: Message) {
    this.handleIncoming(action, outMessage, Unit=>{})
  }

  override def handleIncoming(action: Action, inMessage: Message, next: Unit => Unit) {
    this ! (inMessage, next)
  }


  def act() {
    while (true) {
      receive {

        case (outMessage: OutMessage, next: (Unit => Unit)) =>
          this.id += 1
          outMessage.rendezvous = this.id
          this.messages += (this.id -> outMessage)

          if (this.id > Int.MaxValue)
            this.id = 0

          next()

        case (inMessage: InMessage, next: (Unit => Unit)) =>
          // check for rendez-vous
          if (inMessage.function == MessageType.FUNCTION_RESPONSE) {
            val outMessage = this.messages.remove(inMessage.rendezvous)
            if (outMessage == None) {
              warn("Received a incoming message with a rendez-vous, but with no matching outgoing message: {}", inMessage)
            }

            inMessage.matchingOutMessage = outMessage
          }

          next()

      }
    }
  }
}
