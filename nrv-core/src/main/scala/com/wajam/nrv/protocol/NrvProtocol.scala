package com.wajam.nrv.protocol

import com.wajam.nrv.cluster.Cluster
import com.wajam.nrv.data.Message
import com.wajam.nrv.service.Action
import com.wajam.nrv.transport.netty.NettyTransport
import com.wajam.nrv.transport.nrv.NrvNettyTransportCodecFactory
import com.wajam.nrv.utils.CompletionCallback

/**
 * Default protocol used by NRV. All nodes must have this protocol, since it's
 * used for cluster management.
 */
class NrvProtocol(cluster: Cluster) extends Protocol("nrv", cluster) {

  val transport = new NettyTransport(cluster.localNode.host,
    cluster.localNode.ports.get(name).get,
    this,
    new NrvNettyTransportCodecFactory)

  def start() {
    transport.start()
  }

  def stop() {
    transport.stop()
  }

  override def handleOutgoing(action: Action, message: Message) {
    val node = message.destination(0).node
    transport.sendMessage(node.host, node.ports(name), message, Some(new CompletionCallback {
      def operationCompleted(result: Option[Throwable]) {
        result match {
          //todo proper implementation
          case Some(throwable) => System.out.println(throwable)
          case None =>
        }
      }

    }))
  }

  override def handleMessageFromTransport(message: AnyRef) {
    handleIncoming(null, message.asInstanceOf[Message])
  }
}
