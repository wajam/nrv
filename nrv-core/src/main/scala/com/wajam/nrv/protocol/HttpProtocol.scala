package com.wajam.nrv.protocol

import com.wajam.nrv.cluster.Cluster
import com.wajam.nrv.transport.netty.HttpNettyTransport
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.handler.codec.http._
import java.net.URI
import com.wajam.nrv.data.{OutMessage, InMessage, Message}

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
        msg.serviceName = name
        msg.path = new URI(req.getUri).getPath
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
    message match {
      case req: InMessage => {
        val request = new DefaultHttpRequest(HttpVersion.HTTP_1_1,
          HttpMethod.valueOf(req.method),
          req.serviceName + req.path)
        val sb = new StringBuilder()
        req.keys.foreach(k => (sb.append(k).append(":").append(req.get(k)).append('\n')))
        request.setContent(ChannelBuffers.copiedBuffer(sb.toString().getBytes))
        request
      }
      case response: OutMessage => {
        val response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK)
        response
      }

    }

  }
}
