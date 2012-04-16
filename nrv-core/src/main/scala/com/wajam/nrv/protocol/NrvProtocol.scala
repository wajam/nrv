package com.wajam.nrv.protocol

import com.wajam.nrv.cluster.Cluster
import com.wajam.nrv.service.Action
import com.wajam.nrv.transport.netty.NettyTransport
import com.wajam.nrv.transport.nrv.NrvNettyTransportCodecFactory
import java.net.{InetSocketAddress, URI}
import com.wajam.nrv.data.{MessageType, InRequest, Message}

/**
 * Default protocol used by NRV. All nodes must have this protocol, since it's
 * used for cluster management.
 */
class NrvProtocol(cluster: Cluster) extends Protocol("nrv", cluster) {

  val transport = new NettyTransport(cluster.localNode.host,
    cluster.localNode.ports.get(name).get,
    this,
    new NrvNettyTransportCodecFactory)

  override def getTransport() = transport

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
