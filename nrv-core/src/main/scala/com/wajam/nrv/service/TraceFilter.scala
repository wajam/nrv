package com.wajam.nrv.service

import com.wajam.nrv.data._
import com.wajam.commons.Logging
import java.net.InetSocketAddress
import com.wajam.tracing.{Annotation, Tracer, RpcName, TraceContext}

/**
 * Listen to incoming and and outgoing message and record trace information on the go
 */
object TraceFilter extends MessageHandler with Logging {

  override def handleIncoming(action: Action, message: InMessage) {
    handleIncoming(action, message, () => {})
  }

  override def handleIncoming(action: Action, message: InMessage, next: () => Unit) {

    message.function match {
      // Message is an incoming request. Adopt received trace context. A new trace context will be automatically
      // created by the Tracer if none is present in the message.
      case MessageType.FUNCTION_CALL =>
        val traceContext = getContextFromMessageMetadata(message, action.tracer)
        action.tracer.trace(traceContext) {
          action.tracer.record(Annotation.ServerRecv(toRpcName(action, message)))
          action.tracer.record(Annotation.ServerAddress(toInetSocketAddress(action, message)))
          next()
        }

      // Message is an incoming response to a known outgoing request.
      case MessageType.FUNCTION_RESPONSE if message.matchingOutMessage.isDefined =>

        // Record ClientRecv response in the same context started by ClientSend
        val matchingOutMessage: OutMessage = message.matchingOutMessage.get
        val traceContext = getContextFromMessageMetadata(matchingOutMessage, action.tracer)
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
    handleOutgoing(action, message, () => {})
  }

  override def handleOutgoing(action: Action, message: OutMessage, next: () => Unit) {
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

  def getContextFromMessageMetadata(message: Message, tracer: Tracer): Option[TraceContext] = {
    val traceId: Option[String] = getMetadataValue(message, TraceHeader.TraceId.toString)
    val spanId: Option[String] = getMetadataValue(message, TraceHeader.SpanId.toString)
    val parentId: Option[String] = getMetadataValue(message, TraceHeader.ParentId.toString)
    val sampled = getMetadataValue(message, TraceHeader.Sampled.toString).map(_.toBoolean)

    (traceId, spanId, parentId, sampled) match {
      case (Some(tid), Some(sid), pid, s) => Some(TraceContext(tid, sid, pid, s))
      case (_, _, _, s) => Some(tracer.createRootContext(s)) // Support sampled header without any other header for debuging
    }
  }

  private def getMetadataValue(message: Message, key: String): Option[String] = {
    message.metadata.getOrElse(key, null) match {
      case MList(Seq()) => None
      case MList(Seq(MString(s), _)) => Some(s)
      case MString(value) => Some(value)
      case _ => None
    }
  }

  def setContextInMessageMetadata(message: Message, traceContext: Option[TraceContext]) {
    clearContextInMessageMetadata(message)
    for (context <- traceContext) {
      message.metadata(TraceHeader.TraceId.toString) = context.traceId
      message.metadata(TraceHeader.SpanId.toString) = context.spanId
      for (parentId <- context.parentId) message.metadata(TraceHeader.ParentId.toString) = parentId
      for (sampled <- context.sampled) message.metadata(TraceHeader.Sampled.toString) = sampled.toString
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
    node.protocolsSocketAddress.getOrElse(message.serviceName, node.protocolsSocketAddress(action.protocol.name))
  }

}

object TraceHeader extends Enumeration {
  val TraceId = Value("X-TRC-TRACEID")
  val SpanId = Value("X-TRC-SPANID")
  val ParentId = Value("X-TRC-PARENTID")
  val Sampled = Value("X-TRC-SAMPLED")

  // This is an attachment key, not a metadata key
  val OriginalContext = "X-TRC-ORIGINALCONTEXT"
}

