package com.wajam.nrv.protocol

import com.wajam.nrv.data.{SerializableMessage, Message}
import com.wajam.nrv.cluster.{Node, LocalNode}
import com.wajam.nrv.utils.ExecutorPool

class NrvMemoryProtocol(name: String,
                        localNode: LocalNode,
                        numExecutor: Int = 100) extends Protocol(name, localNode) {

  protected val inMessagePerSecond = metrics.meter("in-message-rate", "messages")
  protected val outMessagePerSecond = metrics.meter("out-message-rate", "messages")

  // Set the right number of executor
  private val executorPool = new ExecutorPool(numExecutor)

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
        transportMessageReceived(message, Some(NrvMemoryConnection), flags)
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

// Protocol required a connection, it's how it distinguish from a request and response. This object is used to mark
// a response of this class.
object NrvMemoryConnection