package com.wajam.nrv.protocol

import com.wajam.nrv.protocol.codec.{Codec, StringCodec}
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.handler.codec.http._
import java.net.URI
import scala.collection.JavaConverters._
import com.wajam.nrv.cluster.Node
import com.wajam.nrv.transport.http.HttpNettyTransport
import scala.Array
import com.wajam.nrv.data.{Message, InMessage, OutMessage}

/**
 * Implementation of HTTP protocol.
 */
class HttpProtocol(name: String, localNode: Node, messageRouter: ProtocolMessageListener)
  extends Protocol(name, messageRouter) {

  val contentTypeCodecs = new collection.mutable.HashMap[String, Codec]
  contentTypeCodecs += ("text/plain" -> new StringCodec())

  override val transport = new HttpNettyTransport(localNode.host,
    localNode.ports(name),
    this)

  def start() {
    transport.start()
  }

  def stop() {
    transport.stop()
  }

  def registerCodec(contentType: String, codec: Codec) {
    contentTypeCodecs += (contentType -> codec)
  }

  override def parse(message: AnyRef): Message = {
    val msg = new InMessage()
    message match {
      case req: HttpRequest => {
        msg.protocolName = "http"
        msg.method = req.getMethod.getName
        msg.serviceName = name
        msg.path = new URI(req.getUri).getPath
        msg.attachments(HttpProtocol.KEEP_ALIVE_KEY) = isKeepAlive(req)
        val nettyUriDecoder = new QueryStringDecoder(req.getUri)
        val parsedParameters = Map.empty[String, Any] ++ nettyUriDecoder.getParameters.asScala.mapValues(_.asScala)
        msg.parameters ++= collapseSingletonLists(parsedParameters)
        mapHeaders(req, msg)
        mapContent(req, msg)
      }
      case res: HttpResponse => {
        msg.protocolName = "http"
        msg.method = null
        msg.serviceName = name
        msg.path = null
        msg.metadata(HttpProtocol.STATUS_CODE_KEY) = res.getStatus.getCode
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
        val code = res.metadata.getOrElse(HttpProtocol.STATUS_CODE_KEY,
          res.metadata.getOrElse(HttpProtocol.STATUS_CODE_KEY, 200)).asInstanceOf[Int]
        val response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.valueOf(code))
        setHeaders(response, res)
        setContent(response, res)
        if(isKeepAlive(response) && res.attachments.getOrElse(HttpProtocol.KEEP_ALIVE_KEY, false).asInstanceOf[Boolean])  {
          res.attachments(Protocol.CLOSE_AFTER) = false
          response.setHeader("Connection", "keep-alive")
        } else {
          res.attachments(Protocol.CLOSE_AFTER) = true
          response.setHeader("Connection", "close")
        }
        response
      }
    }
  }

  def createErrorMessage(inMessage: InMessage, exception: ListenerException) = {
    val errorMessage = new OutMessage()
    errorMessage.metadata(HttpProtocol.STATUS_CODE_KEY) = 404
    errorMessage
  }

  private def mapHeaders(httpMessage: HttpMessage, message: Message) {
    val headers: Map[String, Any] = httpMessage.getHeaderNames.asScala.map { key =>
      key.toUpperCase -> httpMessage.getHeaders(key).asScala
    }.toMap
    message.metadata ++= collapseSingletonLists(headers)
  }

  private def mapContent(httpMessage: HttpMessage, message: Message) {
    val contentTypeHeader = message.metadata.getOrElse("CONTENT-TYPE",
      HttpProtocol.DEFAULT_CONTENT_TYPE).asInstanceOf[String]
    val (contentType, contentEncoding) = splitContentTypeHeader(contentTypeHeader)
    val byteBuffer = httpMessage.getContent.toByteBuffer
    val bytes = new Array[Byte](byteBuffer.limit())
    httpMessage.getContent.readBytes(bytes)
    contentTypeCodecs.get(contentType) match {
      case Some(codec) => message.messageData = codec.decode(bytes, contentEncoding)
      case None => log.warn("Missing codec for content-type {}", contentType)
    }
  }

  private def setHeaders(httpMessage: HttpMessage, message: Message) {
    message.metadata.foreach(e => httpMessage.addHeader(e._1, e._2))
  }

  private def setContent(httpMessage: HttpMessage, message: Message) {
    if(message.messageData != null) {
      val contentTypeHeader = message.metadata.getOrElse("Content-Type",
        HttpProtocol.DEFAULT_CONTENT_TYPE).asInstanceOf[String]
      val (contentType, contentEncoding) = splitContentTypeHeader(contentTypeHeader)
      contentTypeCodecs.get(contentType) match {
        case Some(codec) => {
          val data = codec.encode(message.messageData, contentEncoding)
          httpMessage.setContent(ChannelBuffers.copiedBuffer(data))
          httpMessage.setHeader("Content-Length", data.length)
        }
        case None => {
          log.warn("Missing codec for content-type {}", contentType)
          httpMessage.setHeader("Content-Length", 0)
        }
      }
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

  private def splitContentTypeHeader(contentType: String): (String, String) = {
    val parts = contentType.split(";")
    var found = false
    var charset: String = HttpProtocol.DEFAULT_ENCODING
    for (part:String <- parts if !found) {
      if (part.trim().startsWith("charset=")) {
        val vals = part.split("=");
        if (vals.length > 1) {
          charset = vals(1).trim();
          charset = charset.replaceAll("\"", "").replaceAll("'", "");
          found = true
        }
      }
    }
    (parts(0), charset)
  }

  private def collapseSingletonLists(parameters: Map[String, Any]): Map[String, Any] = {
    parameters.mapValues( v => {
      v match {
        case l:Seq[_] if l.size == 1 => {
          l(0)
        }
        case x => v
      }
    })
  }
}

object HttpProtocol {
  val KEEP_ALIVE_KEY = "httpprotocol-keepalive"
  val STATUS_CODE_KEY = "status-code"
  val DEFAULT_CONTENT_TYPE = "text/plain"
  val DEFAULT_ENCODING = "ISO-8859-1"
}

