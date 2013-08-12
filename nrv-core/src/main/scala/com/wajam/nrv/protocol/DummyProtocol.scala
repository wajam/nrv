package com.wajam.nrv.protocol

import com.wajam.nrv.service.Action
import com.wajam.nrv.data.{OutMessage, Message, InMessage}
import com.wajam.nrv.cluster.{Node, LocalNode}

/**
 * Loopback protocol that always send messages to local process
 */
class DummyProtocol(name: String, localNode: LocalNode) extends Protocol(name, localNode) {

  override def handleOutgoing(action: Action, message: OutMessage) {
    message.protocolName = name
    val newMessage = new InMessage()
    message.copyTo(newMessage)
    this.handleIncoming(action, newMessage)
  }

  def start() {}

  def stop() {}

  def parse(message: AnyRef, flags: Map[String, Any]): Message = null

  def generate(message: Message, flags: Map[String, Any]): AnyRef = null

  def sendMessage(destination: Node, message: AnyRef, closeAfter: Boolean, flags: Map[String, Any], completionCallback: (Option[Throwable]) => Unit) {}

  def sendResponse(connection: AnyRef, message: AnyRef, closeAfter: Boolean, flags: Map[String, Any], completionCallback: (Option[Throwable]) => Unit) {}
}