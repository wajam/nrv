package com.wajam.nrv.protocol

import com.wajam.nrv.cluster.Cluster
import com.wajam.nrv.Logging
import com.wajam.nrv.service.{MessageHandler, Action}
import java.net.InetSocketAddress
import com.wajam.nrv.transport.Transport
import com.wajam.nrv.data.{OutMessage, MessageType, InMessage, Message}

/**
 * Protocol used to send and receive messages to remote nodes over a network
 */
abstract class Protocol(var name: String, cluster: Cluster) extends MessageHandler with Logging {

  val transport:Transport

  /**
   * Start the protocol and the transport layer below it.
   */
  def start()

  /**
   * Stop the protocol and the transport layer below it.
   */
  def stop()

  override def handleIncoming(action: Action, message: InMessage) {
    this.cluster.routeIncoming(message)
  }

  override def handleOutgoing(action: Action, message: OutMessage) {
    val node = message.destination(0).node

    def completionCallback = (result: Option[Throwable]) => {
      result match {
        case Some(throwable) => {
          val response = new InMessage()
          message.copyTo(response)
          response.error = Some(new RuntimeException(throwable))
          response.function = MessageType.FUNCTION_RESPONSE

          handleIncoming(action, response, Unit => {})
        }
        case None =>
      }
    }

    message.attachments.getOrElse(Protocol.CONNECTION_KEY, None).asInstanceOf[Option[AnyRef]] match {
      case Some(channel) => {
        val response = generate(message)
        transport.sendResponse(channel,
          response,
          message.attachments.getOrElse(Protocol.CLOSE_AFTER, false).asInstanceOf[Boolean],
          completionCallback)
      }
      case None => {
        val request = generate(message)
        transport.sendMessage(new InetSocketAddress(node.host, node.ports(name)),
          request,
          message.attachments.getOrElse(Protocol.CLOSE_AFTER, false).asInstanceOf[Boolean],
          completionCallback)
      }
    }
  }

  /**
   * Parse the received message and convert it to a standard Message object.
   *
   * @param message The message received from the network
   * @return The standard Message object that represent the network message
   */
  def parse(message: AnyRef): Message

  /**
   * Generate a transport message from a standard Message object.
   *
   * @param message The standard Message object
   * @return The message to be sent of the network
   */
  def generate(message: Message): AnyRef
}

object Protocol {
  val CONNECTION_KEY = "connection"
  val CLOSE_AFTER = "close_after"
}
