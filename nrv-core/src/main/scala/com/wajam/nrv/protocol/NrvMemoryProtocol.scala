package com.wajam.nrv.protocol

import com.wajam.nrv.data.{SerializableMessage, Message}
import com.wajam.nrv.cluster.{Node, LocalNode}
import com.wajam.nrv.utils.ExecutorPool

class NrvMemoryProtocol(name: String,
                        localNode: LocalNode,
                        numExecutor: Option[Int] = None) extends Protocol(name, localNode) {

  protected val inMessagePerSecond = metrics.meter("in-message-rate", "messages")
  protected val outMessagePerSecond = metrics.meter("out-message-rate", "messages")

  // Set the right number of executor or set default
  private val executorPool =
    if (numExecutor.isDefined)
      new ExecutorPool(numExecutor.get)
    else
      new ExecutorPool()

  def start() {
    executorPool.start
  }

  def stop() {}

  def parse(message: AnyRef, flags: Map[String, Any]): Message = {
    // Noop
    message.asInstanceOf[Message]
  }

  def generate(message: Message, flags: Map[String, Any]) = {
    // Noop, only copy the message, to ensure unwanted field aren't passed to the receiving end
    SerializableMessage(message)
  }

  private def sendMessage(message: AnyRef, flags: Map[String, Any], completionCallback: (Option[Throwable]) => Unit) {
    outMessagePerSecond.mark()

    executorPool.execute {
      () => {
        inMessagePerSecond.mark()
        transportMessageReceived(message, None)
        completionCallback(None)
      }
    }
  }

  def sendMessage(destination: Node,
                  message: AnyRef,
                  closeAfter: Boolean,
                  flags: Map[String, Any],
                  completionCallback: (Option[Throwable]) => Unit) {

    sendMessage(message, flags, completionCallback)
  }

  def sendResponse(connection: AnyRef,
                   message: AnyRef,
                   closeAfter: Boolean,
                   flags: Map[String, Any],
                   completionCallback: Option[Throwable] => Unit = (_) => {}) {

    sendMessage(message, flags, completionCallback)
  }
}

object NrvMemoryProtocol {
  val CONNECTION_KEY = "NrvMemoryProtocol"
}
