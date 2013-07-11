package com.wajam.nrv.protocol

import com.wajam.nrv.data.{SerializableMessage, Message}
import com.wajam.nrv.cluster.{Node, LocalNode}

class NrvMemoryProtocol(name: String,
                        localNode: LocalNode) extends Protocol(name, localNode)  {

  protected val messagePerSecond = metrics.meter("message-rate", "messages")

  def start() {}

  def stop() {}

  def parse(message: AnyRef, flags: Map[String, Any]): Message = {
    // Noop
    message.asInstanceOf[Message]
  }

  def generate(message: Message, flags: Map[String, Any]) = {
    // Noop, only copy the message, to ensure unwanted field aren't passed to the receiving end
    SerializableMessage(message)
  }

  def sendMessage(destination: Node,
                  message: AnyRef,
                  closeAfter: Boolean,
                  flags: Map[String, Any],
                  completionCallback: (Option[Throwable]) => Unit) {

    messagePerSecond.mark()
    transportMessageReceived(message, None)
    completionCallback(None)
  }

  def sendResponse(connection: AnyRef,
                   message: AnyRef,
                   closeAfter: Boolean,
                   flags: Map[String, Any],
                   completionCallback: Option[Throwable] => Unit = (_) => {}) {

    messagePerSecond.mark()
    transportMessageReceived(message, None)
    completionCallback(None)
  }
}

object NrvMemoryProtocol {
  val CONNECTION_KEY = "NrvMemoryProtocol"
}
