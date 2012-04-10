package com.wajam.nrv.protocol

import com.wajam.nrv.cluster.Cluster
import com.wajam.nrv.Logging
import com.wajam.nrv.service.{Action}
import com.wajam.nrv.data.{InRequest, Message}

/**
 * Protocol used to send and receive messages to remote nodes over a network
 */
abstract class Protocol(var name: String, var cluster: Cluster) extends Logging {

  def start()

  def stop()

  def handleOutgoing(action: Action, message: Message)

  def handleIncoming(message: AnyRef) {
    val inReq = new InRequest
    val serMessage = message.asInstanceOf[Message]
    serMessage.copyTo(inReq)

    this.cluster.route(inReq)
  }
}
