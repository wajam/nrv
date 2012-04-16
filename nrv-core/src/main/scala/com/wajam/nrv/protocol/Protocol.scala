package com.wajam.nrv.protocol

import com.wajam.nrv.cluster.Cluster
import com.wajam.nrv.Logging
import com.wajam.nrv.service.{MessageHandler, Action}
import java.net.InetSocketAddress
import com.wajam.nrv.data.{MessageType, InRequest, Message}
import com.wajam.nrv.transport.Transport

/**
 * Protocol used to send and receive messages to remote nodes over a network
 */
abstract class Protocol(var name: String, cluster: Cluster) extends MessageHandler with Logging {

  def start()

  def stop()

  def handleIncoming(action: Action, message: Message) {
    val inReq = new InRequest
    val serMessage = message.asInstanceOf[Message]
    serMessage.copyTo(inReq)

    this.cluster.route(inReq)
  }

  def getTransport(): Transport = null

  override def handleOutgoing(action: Action, message: Message) {
    val node = message.destination(0).node

    getTransport().sendMessage(new InetSocketAddress(node.host, node.ports(name)), generate(message), (result: Option[Throwable]) => {
      result match {
        case Some(throwable) => {
          val response = new InRequest()
          message.copyTo(response)
          response.error = Some(new RuntimeException(throwable))
          response.function = MessageType.FUNCTION_RESPONSE
          handleIncoming(action, response)
        }
        case None =>
      }
    })
  }

  def parse(message: AnyRef) : Message

  def generate(message: Message) : AnyRef
}
