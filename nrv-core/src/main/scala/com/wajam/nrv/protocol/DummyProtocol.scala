package com.wajam.nrv.protocol

import com.wajam.nrv.service.Action
import com.wajam.nrv.cluster.Cluster
import com.wajam.nrv.data.{OutMessage, Message, InMessage}

/**
 * Loopback protocol that always send messages to local process
 */
class DummyProtocol(name: String, messageRouter: ProtocolMessageListener) extends Protocol(name, messageRouter) {

  override val transport = null

  override def handleOutgoing(action: Action, message: OutMessage) {
    message.protocolName = name
    val newMessage = new InMessage()
    message.copyTo(newMessage)
    messageRouter.messageReceived(newMessage)
  }

  def start() {}

  def stop() {}

  def parse(message: AnyRef): Message = null

  def generate(message: Message): AnyRef = null
}