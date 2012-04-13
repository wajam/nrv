package com.wajam.nrv.protocol

import com.wajam.nrv.cluster.Cluster
import com.wajam.nrv.Logging
import com.wajam.nrv.data.{InRequest, Message}
import com.wajam.nrv.service.{MessageHandler, Action}

/**
 * Protocol used to send and receive messages to remote nodes over a network
 */
abstract class Protocol(var name: String, var cluster: Cluster) extends MessageHandler with Logging {

  def start()

  def stop()

  def handleIncoming(action: Action, message: Message) {
    val inReq = new InRequest
    val serMessage = message.asInstanceOf[Message]
    serMessage.copyTo(inReq)

    this.cluster.route(inReq)
  }

  def handleMessageFromTransport(message: AnyRef)
}
