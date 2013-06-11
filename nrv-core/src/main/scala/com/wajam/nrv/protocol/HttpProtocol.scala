package com.wajam.nrv.protocol

import com.wajam.nrv.protocol.codec.{Codec, StringCodec}
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.handler.codec.http._
import java.net.URI
import scala.collection.JavaConverters._
import com.wajam.nrv.cluster.LocalNode
import com.wajam.nrv.transport.http.HttpNettyTransport
import scala.Array
import com.wajam.nrv.data._
import com.wajam.nrv.service.ActionMethod
import com.wajam.nrv.RouteException

/**
 * Implementation of HTTP protocol.
 */
class HttpProtocol(name: String,
                   localNode: LocalNode,
                   idleConnectionTimeoutMs: Long,
                   maxConnectionPoolSize: Int,
                   enableChunkEncoding: Boolean,
                   chunkSize: Int)
  extends Protocol(name) {

  val contentTypeCodecs = new collection.mutable.HashMap[String, Codec]
  contentTypeCodecs += ("text/plain" -> new StringCodec())

  override val transport = new HttpNettyTransport(localNode.listenAddress,
    localNode.ports(name),
    this,
    idleConnectionTimeoutMs,
    maxConnectionPoolSize)

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
        msg.protocolName = name
        msg.method = req.getMethod.getName
        msg.serviceName = HttpHeaders.getHost(req, "")
        msg.path = new URI(req.getUri).getPath
        msg.attachments(HttpProtocol.KEEP_ALIVE_KEY) = isKeepAlive(req)
        msg.code = if (req.getHeader(HttpProtocol.CODE_HEADER) != null) {
          req.getHeader(HttpProtocol.CODE_HEADER).toInt
        } else {
          200
        }
        val nettyUriDecoder = new QueryStringDecoder(req.getUri)
        val parsedParameters = Map.empty[String, Any] ++ nettyUriDecoder.getParameters.asScala.mapValues(_.asScala)
        msg.parameters ++= collapseSingletonLists(parsedParameters)
        mapHeaders(req, msg)
        mapContent(req, msg)
      }
      case res: HttpResponse => {
        msg.protocolName = name
        msg.method = if (res.getHeader(HttpProtocol.METHOD_HEADER) != null) {
          res.getHeader(HttpProtocol.METHOD_HEADER)
        } else {
          ActionMethod.GET
        }
        msg.serviceName = if (res.getHeader(HttpProtocol.SERVICE_HEADER) != null) {
          res.getHeader(HttpProtocol.SERVICE_HEADER)
        } else {
          ""
        }
        msg.path = if (res.getHeader(HttpProtocol.PATH_HEADER) != null) {
          res.getHeader(HttpProtocol.PATH_HEADER)
        } else {
          ""
        }
        msg.code = res.getStatus.getCode
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
        var uri = req.serviceName + req.path
        if (query.length > 0) {
          uri = uri + '?' + query
        }
        val request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.valueOf(req.method), uri)
        setRequestHeaders(request, req)
        setContent(request, req)
        request
      }
      case res: OutMessage => {
        if (enableChunkEncoding) {
          HttpProtocol.HttpChunkedMessage(this, res)
        }
        else {
          val response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.valueOf(res.code))
          setResponseHeaders(response, res)
          setContent(response, res)
          setKeepAlive(response, res)

          response
        }
      }
    }
  }

  override protected def handleIncomingMessageError(exception: Exception, connectionInfo: Option[AnyRef]) {
    handleOutgoing(null, createErrorMessage(exception, exception match {
      case pe: ParsingException => pe.code
      case re: RouteException => 404
      case _ => 500
    }, connectionInfo))
  }

  private def mapHeaders(httpMessage: HttpMessage, message: Message) {
    val headers: Map[String, Any] = httpMessage.getHeaderNames.asScala.map {
      key =>
        key.toUpperCase -> httpMessage.getHeaders(key).asScala
    }.toMap
    message.metadata ++= collapseSingletonLists(headers)
  }

  private def mapContent(httpMessage: HttpMessage, message: Message) {
    val contentTypeHeader = message.metadata.getOrElse("CONTENT-TYPE",
      HttpProtocol.DEFAULT_CONTENT_TYPE).toString
    val (contentType, contentEncoding) = splitContentTypeHeader(contentTypeHeader)
    val byteBuffer = httpMessage.getContent.toByteBuffer
    val bytes = new Array[Byte](byteBuffer.limit())
    httpMessage.getContent.readBytes(bytes)
    contentTypeCodecs.get(contentType) match {
      case Some(codec) => message.messageData = codec.decode(bytes, contentEncoding)
      case None => {
        throw new ParsingException("Unsupported content-type " + contentType, 406)
      }
    }
  }

  private def setRequestHeaders(httpMessage: HttpMessage, message: Message) {
    setHeaders(httpMessage, message)
    httpMessage.addHeader(HttpProtocol.CODE_HEADER, message.code)
  }

  private def setResponseHeaders(httpMessage: HttpMessage, message: Message) {
    setHeaders(httpMessage, message)
    httpMessage.addHeader(HttpProtocol.METHOD_HEADER, message.method)
    httpMessage.addHeader(HttpProtocol.PATH_HEADER, message.path)
    httpMessage.addHeader(HttpProtocol.SERVICE_HEADER, message.serviceName)
    if(enableChunkEncoding) httpMessage.addHeader("Transfer-Encoding", "chunked")
  }

  private def setHeaders(httpMessage: HttpMessage, message: Message) {
    message.metadata.foreach(e => httpMessage.addHeader(e._1, e._2))
  }

  private def setContent(httpMessage: HttpMessage, message: Message) {
    encodeData(message) match {
      case Some(data) => {
        httpMessage.setContent(ChannelBuffers.copiedBuffer(data))
        httpMessage.setHeader("Content-Length", data.length)
      }
      case None => httpMessage.setHeader("Content-Length", 0)
    }
  }

  private def setKeepAlive(response: HttpMessage, res: Message) {
    if (isKeepAlive(response) && res.attachments.getOrElse(HttpProtocol.KEEP_ALIVE_KEY, false).asInstanceOf[Boolean]) {
      res.attachments(Protocol.CLOSE_AFTER) = false
      response.setHeader("Connection", "keep-alive")
    } else {
      res.attachments(Protocol.CLOSE_AFTER) = true
      response.setHeader("Connection", "close")
    }
  }

  // Split an OutMessage in chunks, according to the chunk size set in configuration
  private def getChunks(message: Message): List[DefaultHttpChunk] = {
      encodeData(message) match {
        case Some(data) => {
          data.grouped(chunkSize).map { chunkArray =>
            new DefaultHttpChunk(ChannelBuffers.copiedBuffer(chunkArray))
          }.toList
        }
        case _ => Nil
      }
  }

  private def encodeData(message: Message): Option[Array[Byte]] = {
    if (message.messageData != null) {
      val contentTypeHeader = message.metadata.getOrElse("Content-Type",
        HttpProtocol.DEFAULT_CONTENT_TYPE).toString
      val (contentType, contentEncoding) = splitContentTypeHeader(contentTypeHeader)
      contentTypeCodecs.get(contentType) match {
        case Some(codec) => Some(codec.encode(message.messageData, contentEncoding))
        case None => {
          log.warn("Missing codec for content-type {}", contentType)
          None
        }
      }
    }
    else None
  }

  private def isKeepAlive(message: HttpMessage): Boolean = {
    if (message.getHeader("Connection") == null) {
      message.getProtocolVersion == HttpVersion.HTTP_1_1
    } else if (message.getHeader("Connection").equalsIgnoreCase("keep-alive")) {
      true
    } else {
      false
    }
  }

  private def splitContentTypeHeader(contentType: String): (String, String) = {
    val parts = contentType.split(";")
    var found = false
    var charset: String = HttpProtocol.DEFAULT_ENCODING
    for (part: String <- parts if !found) {
      if (part.trim().startsWith("charset=")) {
        val vals = part.split("=")
        if (vals.length > 1) {
          charset = vals(1).trim()
          charset = charset.replaceAll("\"", "").replaceAll("'", "")
          found = true
        }
      }
    }
    (parts(0), charset)
  }

  private def collapseSingletonLists(parameters: Map[String, Any]): Map[String, MValue] = {
    parameters.mapValues(v => {
      v match {
        case l: Seq[_] if l.size == 1 => {
          MString(l(0).asInstanceOf[String])
        }
        case x => MList(v.asInstanceOf[Iterable[String]].map(MString(_)))
      }
    })
  }

  private def createErrorMessage(exception: Exception, code: Int, connectionInfo: Option[AnyRef]): OutMessage = {
    val errorMessage = new OutMessage()
    errorMessage.code = code
    errorMessage.messageData = exception.getMessage
    errorMessage.attachments(Protocol.CONNECTION_KEY) = connectionInfo
    errorMessage.attachments(Protocol.CLOSE_AFTER) = true
    errorMessage
  }
}

object HttpProtocol {
  val DEFAULT_CONTENT_TYPE = "text/plain"
  val DEFAULT_ENCODING = "ISO-8859-1"
  val KEEP_ALIVE_KEY = "httpprotocol-keepalive"
  val STATUS_CODE_KEY = "status-code"

  val CODE_HEADER = "nrv-code-header"
  val METHOD_HEADER = "nrv-method-header"
  val PATH_HEADER = "nrv-path-header"
  val SERVICE_HEADER = "nrv-service-header"

  class HttpChunkedMessage(val begin: DefaultHttpResponse,
                           val chunks: List[DefaultHttpChunk],
                           val emptyChunk: DefaultHttpChunk,
                           val trailer: DefaultHttpChunkTrailer)

  object HttpChunkedMessage {

    val EMPTY_CHUNK = new DefaultHttpChunk(ChannelBuffers.EMPTY_BUFFER)
    val TRAILER = new DefaultHttpChunkTrailer

    def apply(protocol: HttpProtocol, res: OutMessage) = {
      val begin = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.valueOf(res.code))
      begin.setChunked(true)

      protocol.setResponseHeaders(begin, res)
      protocol.setKeepAlive(begin, res)

      val chunks = protocol.getChunks(res)

      new HttpChunkedMessage(begin, chunks, EMPTY_CHUNK, TRAILER)
    }
  }
}

