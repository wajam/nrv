package com.wajam.nrv.protocol

import com.wajam.nrv.cluster.Cluster
import com.wajam.nrv.transport.netty.HttpNettyTransport
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.handler.codec.http._
import com.wajam.nrv.data.{InMessage, Message}

/**
 * Implementation of HTTP protocol.
 */
class HttpProtocol(name: String, cluster: Cluster) extends Protocol(name, cluster) {

  val transport = new HttpNettyTransport(cluster.localNode.host,
    cluster.localNode.ports(name),
    this)

  def start() {
    transport.start()
  }

  def stop() {
    transport.stop()
  }

  override def getTransport() = transport

  override def parse(message: AnyRef): Message = {
    val msg = new InMessage()
    message match {
      case req: HttpRequest => {
        msg.method = req.getMethod().getName()
        msg.protocolName = "http"
        msg.path = req.getUri()
        // TODO: do more stuff

      }
      case res: HttpResponse => {
        msg.protocolName = "http"
      }
      case _ => throw new RuntimeException("Invalid type: " + message.getClass.getName)
    }
    msg
  }

  override def generate(message: Message): AnyRef = {
    val request = new DefaultHttpRequest(HttpVersion.HTTP_1_1,
      HttpMethod.valueOf(message.method),
      message.serviceName + message.path)
    val sb = new StringBuilder()
    message.keys.foreach(k => (sb.append(k).append(":").append(message.get(k)).append('\n')))
    request.setContent(ChannelBuffers.copiedBuffer(sb.toString().getBytes))
    request
  }
}
