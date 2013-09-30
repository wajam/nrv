package com.wajam.nrv.protocol

import java.io.{ByteArrayInputStream, InputStream}
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import org.jboss.netty.handler.stream.{ChunkedStream, ChunkedInput}
import org.jboss.netty.handler.codec.http._
import java.net.URI
import scala.collection.JavaConverters._
import com.wajam.nrv.protocol.codec.{Codec, StringCodec}
import com.wajam.nrv.cluster.{Node, LocalNode}
import com.wajam.nrv.transport.http.HttpNettyTransport
import com.wajam.nrv.data._
import com.wajam.nrv.service.ActionMethod
import com.wajam.nrv.RouteException
import com.wajam.nrv.data.MessageType._
import HttpProtocol.{HttpChunkedResponse, HttpChunkedInputStream, HttpChunkedByteStream}
import scala.Some
import com.wajam.nrv.data.MList
import com.wajam.nrv.data.MString
import com.wajam.nrv.protocol.ParsingException

/**
 * Implementation of HTTP protocol.
 */
class HttpProtocol(name: String,
                   localNode: LocalNode,
                   idleConnectionTimeoutMs: Long,
                   maxConnectionPoolSize: Int,
                   chunkSize: Option[Int] = None)
  extends Protocol(name, localNode) {

  private val contentTypeCodecs = new collection.mutable.HashMap[String, Codec]
  contentTypeCodecs += ("text/plain" -> new StringCodec())

  private val enableChunkEncoding = !chunkSize.isEmpty

  val transport = new HttpNettyTransport(localNode.listenAddress,
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

  override def parse(message: AnyRef, flags: Map[String, Any]): Message = {
    val msg = new InMessage()
    message match {
      case req: HttpRequest => {
        msg.protocolName = name
        msg.method = req.getMethod.getName
        msg.path = new URI(req.getUri).getPath
        msg.function = FUNCTION_CALL
        msg.attachments(HttpProtocol.KEEP_ALIVE_KEY) = isKeepAlive(req)
        msg.code = getHeaderValue(req, HttpProtocol.CODE_HEADER, "200").toInt
        extractRendezVousFromHeaders(req, msg)
        val nettyUriDecoder = new QueryStringDecoder(req.getUri)
        val parsedParameters = Map.empty[String, Any] ++ nettyUriDecoder.getParameters.asScala.mapValues(_.asScala)
        msg.parameters ++= collapseSingletonLists(parsedParameters)
        mapHeaders(req, msg)
        mapContent(req, msg)
      }
      case res: HttpResponse => {
        msg.protocolName = name
        msg.method = getHeaderValue(res, HttpProtocol.METHOD_HEADER, ActionMethod.GET)
        msg.path = getHeaderValue(res, HttpProtocol.PATH_HEADER, "")
        msg.function = FUNCTION_RESPONSE
        msg.serviceName = getHeaderValue(res, HttpProtocol.SERVICE_HEADER, "")
        extractRendezVousFromHeaders(res, msg)
        msg.code = res.getStatus.getCode
        mapHeaders(res, msg)
        mapContent(res, msg)
      }
      case _ => throw new RuntimeException("Invalid type: " + message.getClass.getName)
    }
    msg
  }

  def generate(message: Message, flags: Map[String, Any]): AnyRef = {
    generate(message)
  }

  private def generate(message: Message): AnyRef = {
    message.function match {
      case FUNCTION_CALL => {
        val query = new StringBuilder()
        message.parameters.foreach(e => {
          query.append(e._1).append("=").append(e._2).append('&')
        })

        val path = if(message.path.startsWith("/")) message.path else "/" + message.path
        val uri = if (query.length > 0) {
          path + "?" + query.toString()
        } else {
          path
        }
        val request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.valueOf(getHttpMethod(message.method)), uri)
        setRequestHeaders(request, message)
        setAllContentAtOnce(request, encodeData(message))
        request
      }
      case FUNCTION_RESPONSE => {
        message.messageData match {
          case is: InputStream =>
            val stream = HttpChunkedInputStream(is)
            val response = HttpChunkedResponse(message, stream)
            setResponseHeaders(response.begin, message)
            setKeepAlive(response.begin, message)

            response
          case _ =>
            encodeData(message) match {
              case Some(data: Array[Byte]) if enableChunkEncoding && data.length > chunkSize.get =>
                val stream = HttpChunkedByteStream(data, chunkSize.get)
                val response = HttpChunkedResponse(message, stream)
                setResponseHeaders(response.begin, message)
                setKeepAlive(response.begin, message)

                response
              case maybeData =>
                val response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.valueOf(message.code))
                setResponseHeaders(response, message)
                setAllContentAtOnce(response, maybeData)
                setKeepAlive(response, message)

                response
            }
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
    httpMessage.addHeader(HttpProtocol.RENDEZ_VOUS_ID, message.rendezvousId)
  }

  private def setResponseHeaders(httpMessage: HttpMessage, message: Message) {
    setHeaders(httpMessage, message)
    httpMessage.addHeader(HttpProtocol.METHOD_HEADER, message.method)
    httpMessage.addHeader(HttpProtocol.PATH_HEADER, message.path)
    httpMessage.addHeader(HttpProtocol.SERVICE_HEADER, message.serviceName)
    httpMessage.addHeader(HttpProtocol.RENDEZ_VOUS_ID, message.rendezvousId)
  }

  private def setHeaders(httpMessage: HttpMessage, message: Message) {
    message.metadata.foreach(e => httpMessage.addHeader(e._1, e._2))
  }

  private def setAllContentAtOnce(httpMessage: HttpMessage, maybeData: Option[Array[Byte]]) {
    maybeData match {
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
    errorMessage.metadata += (("Content-Type", new MString(HttpProtocol.DEFAULT_CONTENT_TYPE + "; charset=" + HttpProtocol.DEFAULT_ENCODING)))
    errorMessage
  }

  private def getHttpMethod(method: String): String = {
    method match {
      case ActionMethod.POST | ActionMethod.PUT | ActionMethod.DELETE => method
      case _ => "GET"
    }
  }

  private def getHeaderValue(httpMessage: HttpMessage, headerName: String, defaultIfAbsent: String): String = {
    if (httpMessage.getHeader(headerName) != null) {
      httpMessage.getHeader(headerName)
    } else {
      defaultIfAbsent
    }
  }

  private def extractRendezVousFromHeaders(req: HttpMessage, msg: InMessage) {
    if (req.getHeader(HttpProtocol.RENDEZ_VOUS_ID) != null) {
      msg.rendezvousId = req.getHeader(HttpProtocol.RENDEZ_VOUS_ID).toInt
    }
  }

  def sendMessage(destination: Node,
                  message: AnyRef,
                  closeAfter: Boolean,
                  flags: Map[String, Any],
                  completionCallback: (Option[Throwable]) => Unit) {
    transport.sendMessage(destination.protocolsSocketAddress(name), message, closeAfter, completionCallback)
  }

  def sendResponse(connection: AnyRef,
                   message: AnyRef,
                   closeAfter: Boolean,
                   flags: Map[String, Any],
                   completionCallback: (Option[Throwable]) => Unit) {
    transport.sendResponse(connection, message, closeAfter, completionCallback)
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
  val RENDEZ_VOUS_ID = "nrv-rendez-vous-id"

  val CHUNK_EMPTY = new DefaultHttpChunk(ChannelBuffers.EMPTY_BUFFER)
  val CHUNK_TRAILER = new DefaultHttpChunkTrailer

  case class HttpChunkedResponse(res: Message, input: ChunkedInput) {
    val begin: DefaultHttpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.valueOf(res.code))
    begin.addHeader("Transfer-Encoding", "chunked")
    begin.setChunked(true)
  }

  trait HttpChunkedStream extends ChunkedStream {

    override def nextChunk(): HttpChunk = {
      val next = super.nextChunk()

      if(next == null)
        // Should not happen if used with ChunkedWriteHandler
        CHUNK_EMPTY
      else
        new DefaultHttpChunk(next.asInstanceOf[ChannelBuffer])
    }
  }

  case class HttpChunkedInputStream(inputStream: InputStream)
    extends ChunkedStream(inputStream)
    with HttpChunkedStream

  case class HttpChunkedByteStream(bytes: Array[Byte], chunkSize: Int)
    extends ChunkedStream(new ByteArrayInputStream(bytes), chunkSize)
    with HttpChunkedStream

}

