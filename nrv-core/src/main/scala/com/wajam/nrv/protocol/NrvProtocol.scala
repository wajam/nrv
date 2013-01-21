package com.wajam.nrv.protocol

import codec.{MessageJavaSerializeCodec, Codec}
import com.wajam.nrv.data.Message
import com.wajam.nrv.transport.nrv.NrvNettyTransport
import com.wajam.nrv.cluster.LocalNode

/**
 * Default protocol used by NRV. All nodes must have this protocol, since it's
 * used for cluster management.
 */
class NrvProtocol(localNode: LocalNode, codec: Codec = new MessageJavaSerializeCodec)
  extends Protocol("nrv") {

  override val transport = new NrvNettyTransport(localNode.listenAddress, localNode.ports(name), this)

  def start() {
    transport.start()
  }

  def stop() {
    transport.stop()
  }

  def parse(message: AnyRef): Message = {
    codec.decode(message.asInstanceOf[Array[Byte]]).asInstanceOf[Message]
  }

  def generate(message: Message): AnyRef = {
    codec.encode(message)
  }
}
