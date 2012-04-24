package com.wajam.nrv.protocol

import com.wajam.nrv.cluster.Cluster
import com.wajam.nrv.transport.netty.HttpNettyTransport
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.handler.codec.http._
import java.net.URI
import com.wajam.nrv.data.{OutMessage, InMessage, Message}
import scala.collection.JavaConverters._

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
        val uri = new URI(req.getUri)
        msg.protocolName = "http"
        msg.method = req.getMethod.getName
        msg.serviceName = name
        msg.path = uri.getPath
        val nettyUri = new QueryStringDecoder(uri)
        val requestParams = Map.empty[String, Seq[String]] ++ nettyUri.getParameters.asScala.mapValues(_.asScala)
        msg.parameters ++= requestParams
        getHeaders(req, msg)
        getContent(req, msg)
      }
      case res: HttpResponse => {
        msg.protocolName = "http"
        msg.method = null
        msg.serviceName = name
        msg.path = null
        getHeaders(res, msg)
        getContent(res, msg)
      }
      case _ => throw new RuntimeException("Invalid type: " + message.getClass.getName)
    }
    msg
  }

  override def generate(message: Message): AnyRef = {
    message match {
      case req: InMessage => {
        val query = new StringBuilder()
        req.parameters.foreach(e => {
          query.append(e._1).append("=").append(e._2).append('&')
        })

        val request = new DefaultHttpRequest(HttpVersion.HTTP_1_1,
          HttpMethod.valueOf(req.method),
          req.serviceName + req.path + '?' + query)
        setHeaders(request, req)
        setContent(request, req)
        request
      }
      case res: OutMessage => {
        val response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK)
        setHeaders(response, res)
        setContent(response, res)
        response
      }
    }
  }

  private def getHeaders(httpMessage: HttpMessage, message: Message) {
    httpMessage.getHeaders.asScala.foreach(entry => message.metadata.put(entry.getKey, entry.getValue))
  }

  private def getContent(httpMessage: HttpMessage, message: Message) {
    message.messageData = httpMessage.getContent  //todo manage content
  }

  private def setHeaders(httpMessage: HttpMessage, message: Message) {
    message.metadata.foreach(e => httpMessage.addHeader(e._1, e._2))
  }

  private def setContent(httpMessage: HttpMessage, message: Message) {
    if(message.messageData != null) {
      httpMessage.setContent(ChannelBuffers.copiedBuffer(message.messageData.toString.getBytes)) //todo manage content
      httpMessage.setHeader("Content-Length", message.messageData.toString.getBytes.length)
    } else {
      httpMessage.setHeader("Content-Length", 0)
    }
  }
}

