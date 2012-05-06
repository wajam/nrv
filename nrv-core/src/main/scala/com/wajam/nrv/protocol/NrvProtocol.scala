package com.wajam.nrv.protocol

import codec.{JavaSerializeCodec, Codec}
import com.wajam.nrv.data.{OutMessage, InMessage, Message}
import com.wajam.nrv.transport.nrv.NrvNettyTransport
import com.wajam.nrv.cluster.Node

/**
 * Default protocol used by NRV. All nodes must have this protocol, since it's
 * used for cluster management.
 */
class NrvProtocol(localNode: Node, messageRouter: ProtocolMessageListener, codec: Codec[Message] = new JavaSerializeCodec)
  extends Protocol("nrv", messageRouter) {

  override val transport = new NrvNettyTransport(localNode.host,
    localNode.ports(name),
    this)

  def start() {
    transport.start()
  }

  def stop() {
    transport.stop()
  }

  def parse(message: AnyRef): Message = {
    codec.decode(message.asInstanceOf[Array[Byte]])
  }

  def generate(message: Message): AnyRef = {
    codec.encode(message)
  }

  def createErrorMessage(inMessage: InMessage, exception: ListenerException) = {
    val errorMessage = new OutMessage()
    inMessage.copyTo(errorMessage)
    errorMessage.metadata("status") = "error"
    errorMessage
  }
}
