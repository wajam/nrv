package com.wajam.nrv.service

import com.wajam.nrv.data.{Message, MessageType, InMessage, OutMessage}
import com.wajam.nrv.tracing.{RpcName, TraceContext, Tracer, Annotation}
import com.wajam.nrv.Logging
import java.net.{Inet4Address, NetworkInterface, InetAddress, InetSocketAddress}

/**
 * Listen to incoming and and outgoing message and record trace information on the go
 */
object TraceFilter extends MessageHandler with Logging {

  override def handleIncoming(action: Action, message: InMessage) {
    handleIncoming(action, message, _ => {})
  }

  override def handleIncoming(action: Action, message: InMessage, next: (Unit) => Unit) {

    message.function match {
      // Message is an incoming request. Adopt received trace context. A new trace context will be automatically
      // created by the Tracer if none is present in the message.
      case MessageType.FUNCTION_CALL =>
        val traceContext = getContextFromMessageMetadata(message)
        action.tracer.trace(traceContext) {
          action.tracer.record(Annotation.ServerRecv(toRpcName(action, message)))
          action.tracer.record(Annotation.ServerAddress(toInetSocketAddress(action, message)))
          next()
        }

      // Message is an incoming response to a known outgoing request.
      case MessageType.FUNCTION_RESPONSE if message.matchingOutMessage.isDefined =>

        // Record ClientRecv response in the same context started by ClientSend
        val matchingOutMessage: OutMessage = message.matchingOutMessage.get
        val traceContext = getContextFromMessageMetadata(matchingOutMessage)
        action.tracer.trace(traceContext) {
          action.tracer.record(Annotation.ClientRecv(Some(message.code)))
        }

        // Process response logic in the original trace context i.e. prior ClientSend
        val originalContext = matchingOutMessage.attachments.getOrElse(TraceHeader.OriginalContext,
          traceContext.get).asInstanceOf[TraceContext]
        action.tracer.trace(Some(originalContext)) {
          next()
        }

      // Message is an incoming response but it has no known outgoing request matching it. Response likely arrived
      // after a timeout i.e. the reponse took too long to come back and we stoped waiting for it. Too bad!!!
      case _ =>
        next()
    }
  }

  override def handleOutgoing(action: Action, message: OutMessage) {
    handleOutgoing(action, message, _ => {})
  }

  override def handleOutgoing(action: Action, message: OutMessage, next: (Unit) => Unit) {
    message.function match {
      // Message is a call to an external service. Create a sub context (i.e. new span) for the call.
      case MessageType.FUNCTION_CALL =>
        val originalContext = message.attachments.get(TraceHeader.OriginalContext).asInstanceOf[Option[TraceContext]]
        val traceContext = for (context <- originalContext) yield action.tracer.createSubcontext(originalContext.get)
        action.tracer.trace(traceContext) {
          setContextInMessageMetadata(message, action.tracer.currentContext) // Set trace context metadata in request message
          action.tracer.record(Annotation.ClientSend(toRpcName(action, message)))
          action.tracer.record(Annotation.ClientAddress(toInetSocketAddress(action, message)))
          next()
        }

      // Message is a response for an external service. We are already in an trace context.
      case _ =>
        // Clear trace context in response message metadata and attachement
        clearContextInMessageMetadata(message)
        message.attachments.remove(TraceHeader.OriginalContext)
        for (context <- action.tracer.currentContext) {
          action.tracer.record(Annotation.ServerSend(Some(message.code)))
        }
        next()
    }
  }

  def getContextFromMessageMetadata(message: Message): Option[TraceContext] = {
    val traceId: Option[String] = getMetadataValue(message, TraceHeader.TraceId.toString)
    val spanId: Option[String] = getMetadataValue(message, TraceHeader.SpanId.toString)
    val parentSpanId: Option[String] = getMetadataValue(message, TraceHeader.ParentSpanId.toString)

    if (traceId.isDefined && spanId.isDefined)
      Some(TraceContext(traceId.get, spanId.get, parentSpanId))
    else
      None
  }

  private def getMetadataValue(message: Message, key: String): Option[String] = {
    message.metadata.getOrElse(key, null) match {
      case None => None
      case values: Seq[_] if values.isEmpty => None
      case values: Seq[_] => Some(values(0).toString)
      case value: String => Some(value)
      case _ => None
    }
  }

  def setContextInMessageMetadata(message: Message, traceContext: Option[TraceContext]) {
    clearContextInMessageMetadata(message)
    for (context <- traceContext) {
      message.metadata(TraceHeader.TraceId.toString) = context.traceId
      message.metadata(TraceHeader.SpanId.toString) = context.spanId
      for (parentSpanId <- context.parentSpanId) {
        message.metadata(TraceHeader.ParentSpanId.toString) = parentSpanId
      }
    }
  }

  def clearContextInMessageMetadata(message: Message) {
    TraceHeader.values.foreach(message.metadata -= _.toString)
  }

  /**
   * Creates a new RpcName annotation from the specified message information
   */
  private def toRpcName(action: Action, message: Message): RpcName = {
    RpcName(message.serviceName, message.protocolName, message.method, action.path)
  }

  private def toInetSocketAddress(action: Action, message: Message): InetSocketAddress = {
    val node = action.cluster.localNode
    val addr = if (!node.host.isAnyLocalAddress)
      node.host
    else
      localInetAddress.getOrElse(node.host)

    new InetSocketAddress(addr, node.ports.getOrElse(message.serviceName, node.ports(action.protocol.name)))
  }

  lazy val localInetAddress = firstInetAddress

  private def firstInetAddress: Option[InetAddress] = {
    import scala.collection.JavaConversions._
    val nic = NetworkInterface.getNetworkInterfaces.find(nic => !nic.isLoopback && nic.isUp)
    nic match {
      case Some(n) => n.getInetAddresses.find(_.isInstanceOf[Inet4Address])
      case _ => None
    }
  }
}

object TraceHeader extends Enumeration {
  val TraceId = Value("X-TRACEID")
  val SpanId = Value("X-SPANID")
  val ParentSpanId = Value("X-PARENTSPANID")

  // This is an attachment key, not a metadata key
  val OriginalContext = "X-ORIGINALCONTEXT"
}

