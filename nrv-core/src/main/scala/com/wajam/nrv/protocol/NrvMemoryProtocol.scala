package com.wajam.nrv.protocol

import com.wajam.nrv.data.{SerializableMessage, Message}
import com.wajam.nrv.cluster.LocalNode
import java.net.InetSocketAddress

class NrvMemoryProtocol(name: String,
                        localNode: LocalNode) extends Protocol(name, localNode)  {

  def start() {}

  def stop() {}

  def parse(message: AnyRef): Message = {
    // Noop
    message.asInstanceOf[Message]
  }

  def generate(message: Message): AnyRef = {
    // Noop, only copy the message, to ensure unwanted field aren't passed to the receiving end
    SerializableMessage(message)
  }

  def sendMessage(destination: InetSocketAddress,
                  message: AnyRef,
                  closeAfter: Boolean,
                  completionCallback: (Option[Throwable]) => Unit) {
    transportMessageReceived(message, None)
    completionCallback(None)
  }

  def sendResponse(connection: AnyRef,
                   message: AnyRef,
                   closeAfter: Boolean,
                   completionCallback: Option[Throwable] => Unit = (_) => {}) {
    transportMessageReceived(message, None)
    completionCallback(None)
  }
}
