package com.wajam.nrv.protocol

import com.wajam.nrv.transport.netty.NettyTransport
import com.wajam.nrv.transport.nrv.NrvNettyTransportCodecFactory
import com.wajam.nrv.data.Message
import com.wajam.nrv.cluster.{Node, Cluster}

/**
 * Default protocol used by NRV. All nodes must have this protocol, since it's
 * used for cluster management.
 */
class NrvProtocol(localNode: Node, messageRouter: ProtocolMessageListener)
  extends Protocol("nrv", messageRouter) {

  override val transport = new NettyTransport(localNode.host,
    localNode.ports(name),
    this,
    new NrvNettyTransportCodecFactory)

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
}
