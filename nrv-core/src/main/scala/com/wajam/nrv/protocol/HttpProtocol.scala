package com.wajam.nrv.protocol

import com.wajam.nrv.cluster.Cluster
import com.wajam.nrv.service.Action
import com.wajam.nrv.transport.netty.HttpNettyTransport
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.handler.codec.http._
import com.wajam.nrv.data.{InRequest, Message}

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
    val request = new DefaultHttpRequest(HttpVersion.HTTP_1_1,
      HttpMethod.valueOf(message.method),
      message.serviceName+message.path)
    val sb = new StringBuilder()
    message.keys.foreach(k => (sb.append(k).append(":").append(message.get(k)).append('\n')))
    request.setContent(ChannelBuffers.copiedBuffer(sb.toString().getBytes))
    transport.sendMessage(node.host, node.ports(name), request)
  }

  def handleMessageFromTransport(message: AnyRef) {
    val msg = new InRequest()
    message match {
      case req: HttpRequest => {
        msg.method = req.getMethod.getName()
        msg.protocolName = "http"
        msg.path = req.getUri()
        //todo do more stuff

      }
      case res: HttpResponse => {
        msg.protocolName = "http"
      }
      case _ => throw new RuntimeException("Invalid type: " + message.getClass.getName)
    }
    handleIncoming(null, msg)
  }
}
