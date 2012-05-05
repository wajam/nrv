package com.wajam.nrv.protocol

import com.wajam.nrv.transport.netty.NettyTransport
import com.wajam.nrv.cluster.{Node, Cluster}
import com.wajam.nrv.data.{OutMessage, InMessage, Message}
import com.wajam.nrv.transport.nrv.{NrvNettyTransport, NrvNettyTransportCodecFactory}
import com.wajam.nrv.transport.nrv.codec.{JavaSerializeCodec, Codec}

/**
 * Default protocol used by NRV. All nodes must have this protocol, since it's
 * used for cluster management.
 */
class NrvProtocol(localNode: Node, messageRouter: ProtocolMessageListener, codec: Codec = new JavaSerializeCodec)
  extends Protocol("nrv", messageRouter) {

  override val transport = new NrvNettyTransport(localNode.host,
    localNode.ports(name),
    this,
    codec)

  def start() {
    transport.start()
  }

  def stop() {
    transport.stop()
  }

  def parse(message: AnyRef): Message = {
    message.asInstanceOf[Message]
  }

  def generate(message: Message): AnyRef = {
    message
  }

  def createErrorMessage(inMessage: InMessage, exception: ListenerException) = {
    val errorMessage = new OutMessage()
    inMessage.copyTo(errorMessage)
    errorMessage.metadata("status") = "error"
    errorMessage
  }
}
