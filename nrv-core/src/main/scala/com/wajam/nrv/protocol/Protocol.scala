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

  def start()

  def stop()

  override def handleIncoming(action: Action, message: InMessage) {
    this.cluster.routeIncoming(message)
  }

  def getTransport(): Transport = null

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
        getTransport().sendResponse(channel, generate(message), completionCallback)
      }
      case None => {
        getTransport().sendMessage(new InetSocketAddress(node.host, node.ports(name)),
          generate(message), completionCallback)
      }
    }
  }

  def parse(message: AnyRef): Message

  def generate(message: Message): AnyRef
}

object Protocol {
  val CONNECTION_KEY = "connection"
}
