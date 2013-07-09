package com.wajam.nrv.protocol

import com.wajam.nrv.data.{SerializableMessage, Message}
import com.wajam.nrv.cluster.{Node, LocalNode}

class NrvMemoryProtocol(name: String,
                        localNode: LocalNode) extends Protocol(name, localNode)  {

  def start() {}

  def stop() {}

  def parse(message: AnyRef, connection: AnyRef): Message = {
    // Noop
    message.asInstanceOf[Message]
  }

  private def generate(message: Message) = {
    // Noop, only copy the message, to ensure unwanted field aren't passed to the receiving end
    SerializableMessage(message)
  }

  def generateMessage(message: Message, destination: Node): AnyRef = {
    generate(message)
  }

  def generateResponse(message: Message, connection: AnyRef): AnyRef = {
    generate(message)
  }

  def sendMessage(destination: Node,
                  message: AnyRef,
                  closeAfter: Boolean,
                  completionCallback: (Option[Throwable]) => Unit) {
    transportMessageReceived(message, Some(NrvMemoryProtocol.CONNECTION_KEY))
    completionCallback(None)
  }

  def sendResponse(connection: AnyRef,
                   message: AnyRef,
                   closeAfter: Boolean,
                   completionCallback: Option[Throwable] => Unit = (_) => {}) {
    transportMessageReceived(message, Some(NrvMemoryProtocol.CONNECTION_KEY))
    completionCallback(None)
  }
}

object NrvMemoryProtocol {
  val CONNECTION_KEY = "NrvMemoryProtocol"
}
