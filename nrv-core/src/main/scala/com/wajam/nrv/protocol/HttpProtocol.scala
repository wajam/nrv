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

  val KEEP_ALIVE_KEY = "httpprotocol-keepalive"

  override val transport = new HttpNettyTransport(cluster.localNode.host,
    cluster.localNode.ports(name),
    this)

  def start() {
    transport.start()
  }

  def stop() {
    transport.stop()
  }

  override def parse(message: AnyRef): Message = {
    val msg = new InMessage()
    message match {
      case req: HttpRequest => {
        msg.protocolName = "http"
        msg.method = req.getMethod.getName
        msg.serviceName = name
        msg.path = new URI(req.getUri).getPath
        msg.attachments(KEEP_ALIVE_KEY) = isKeepAlive(req)
        val nettyUriDecoder = new QueryStringDecoder(req.getUri)
        msg.parameters ++= Map.empty[String, Seq[String]] ++ nettyUriDecoder.getParameters.asScala.mapValues(_.asScala)
        mapHeaders(req, msg)
        mapContent(req, msg)
      }
      case res: HttpResponse => {
        msg.protocolName = "http"
        msg.method = null
        msg.serviceName = name
        msg.path = null
        mapHeaders(res, msg)
        mapContent(res, msg)
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
        if(isKeepAlive(response) && message.attachments(KEEP_ALIVE_KEY).asInstanceOf[Boolean])  {
          message.attachments(Protocol.CLOSE_AFTER) = false
          response.setHeader("Connection", "keep-alive")
        } else {
          message.attachments(Protocol.CLOSE_AFTER) = true
          response.setHeader("Connection", "close")
        }
        response
      }
    }
  }

  private def mapHeaders(httpMessage: HttpMessage, message: Message) {
    val headers: Map[String, Seq[String]] = httpMessage.getHeaderNames.asScala.map { key =>
      key.toUpperCase -> httpMessage.getHeaders(key).asScala
    }.toMap
    message.metadata ++= headers
  }

  private def mapContent(httpMessage: HttpMessage, message: Message) {
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

  private def isKeepAlive(message: HttpMessage): Boolean = {
    if(message.getHeader("Connection") == null) {
      message.getProtocolVersion == HttpVersion.HTTP_1_1
    } else if(message.getHeader("Connection").equalsIgnoreCase("keep-alive")) {
      true
    } else {
      false
    }
  }
}

