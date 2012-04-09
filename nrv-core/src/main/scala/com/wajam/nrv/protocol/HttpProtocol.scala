package com.wajam.nrv.protocol

import com.wajam.nrv.cluster.Cluster
import com.wajam.nrv.service.Action
import com.wajam.nrv.data.Message
import com.wajam.nrv.transport.netty.HttpNettyTransport

/**
 * This class...
 *
 * User: felix
 * Date: 09/04/12
 */

class HttpProtocol(name: String, cluster: Cluster) extends Protocol(name, cluster) {

  val transport = new HttpNettyTransport(cluster.localNode.host,
    cluster.localNode.ports.get(name).get,
    this)

  def start() {
    transport.start()
  }

  def stop() {
    transport.stop()
  }


  def handleOutgoing(action: Action, message: Message) {
    val node = message.destination(0).node
    //todo convert to netty HttpRequest
    transport.sendMessage(node.host, node.ports(name), message)
  }

  def handleIncoming(message: AnyRef) {}
}
