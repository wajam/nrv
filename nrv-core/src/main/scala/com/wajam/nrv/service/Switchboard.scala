package com.wajam.nrv.service

import actors.Actor
import collection.mutable.Map
import com.wajam.nrv.Logging
import com.yammer.metrics.scala.Instrumented
import com.wajam.nrv.data.{MessageType, InMessage, OutMessage}

/**
 * Handle incoming messages and find matching outgoing messages, having same
 * rendez-vous number.
 */
class Switchboard extends Actor with Logging with Instrumented {
  private var messages = Map[Int, OutMessage]()
  private var id = 0

  private val rdvGauge = metrics.gauge("pending-rendezvous") {
    messages.size
  }

  // TODO: timeouts (w/cleanup)

  def keepOutgoing(outMessage: OutMessage) {
    this !? outMessage
  }

  def matchIncoming(inMessage: InMessage, callback: Option[OutMessage] => Unit) {
    this !(inMessage, callback)
  }


  def act() {
    while (true) {
      receive {

        case outMessage: OutMessage =>
          this.id += 1
          outMessage.rendezvous = this.id
          this.messages += (this.id -> outMessage)

          if (this.id > Int.MaxValue)
            this.id = 0

          sender ! true

        case (inMessage: InMessage, callback: (Option[OutMessage] => Unit)) =>
          // check for rendez-vous
          if (inMessage.function == MessageType.FUNCTION_RESPONSE) {
            val optReq = this.messages.remove(inMessage.rendezvous)
            if (optReq == None) {
              warn("Received a incoming message with a rendez-vous, but with no matching outgoing message: {}", inMessage)
            }
            callback(optReq)
          } else {
            callback(None)
          }

          sender ! true

      }
    }
  }
}
