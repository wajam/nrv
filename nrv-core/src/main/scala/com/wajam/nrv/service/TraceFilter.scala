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
      // Message is an incomming request. Adopt received trace context. A new trace context will be created if none
      // is found from the message.
      case MessageType.FUNCTION_CALL =>
        val traceContext = getContextFromMessage(message)
        action.tracer.trace(traceContext) {
          clearContextInMessage(message) // Clear trace context metadata in request message
          action.tracer.record(Annotation.ServerRecv(toRpcName(action, message)))
          action.tracer.record(Annotation.ServerAddress(toInetSocketAddress(action, message)))
          next()
        }

      // Message is an incoming response to a known ourgoing request. Use matching outgoing request trace context
      case MessageType.FUNCTION_RESPONSE if message.matchingOutMessage.isDefined =>
        val traceContext: Option[TraceContext] = getContextFromMessage(message.matchingOutMessage.get)
        action.tracer.trace(traceContext) {
          action.tracer.record(Annotation.ClientRecv(Some(message.code)))
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
      // Message is a call to an external service. Create a sub context (i.e. new span) for the call
      case MessageType.FUNCTION_CALL =>
        val traceContext = createSubcontextFromMessage(message, action.tracer)
        if (traceContext.isEmpty) {
          // TODO: Fail with an exception once trace context propagation is integrated in all services
          debug("Outgoing request has not trace context! {}", toRpcName(action, message))
        }

        action.tracer.trace(traceContext) {
          setContextInMessage(message, action.tracer.currentContext) // Set trace context metadata in request message
          action.tracer.record(Annotation.ClientSend(toRpcName(action, message)))
          action.tracer.record(Annotation.ClientAddress(toInetSocketAddress(action, message)))
          next()
        }

      case _ =>
        // Message is a response for an external service. Already in an trace context.
        clearContextInMessage(message) // Clear trace context metadata in response message

        if (action.tracer.currentContext.isEmpty) {
          // TODO: Fail with an exception once trace context propagation is integrated in all services
          debug("Outgoing response has not trace context! {}", toRpcName(action, message))
        } else {
          action.tracer.record(Annotation.ServerSend(Some(message.code)))
        }
        next()
    }
  }

  def getContextFromMessage(message: Message): Option[TraceContext] = {
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

  def setContextInMessage(message: Message, context: Option[TraceContext]) {
    clearContextInMessage(message)

    if (context.isDefined) {
      message.metadata(TraceHeader.TraceId.toString) = context.get.traceId
      message.metadata(TraceHeader.SpanId.toString) = context.get.spanId
      if (context.get.parentSpanId.isDefined)
        message.metadata(TraceHeader.ParentSpanId.toString) = context.get.parentSpanId
    }
  }

  def clearContextInMessage(message: Message) {
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

    new InetSocketAddress(addr, node.ports.getOrElse(message.serviceName, node.ports(message.protocolName)))
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

  /**
   * Creates a new trace context using the specified message trace context as parent. Returns None if the message has
   * has no trace context
   */
  private def createSubcontextFromMessage(message: Message, tracer: Tracer): Option[TraceContext] = {
    val parentContext: Option[TraceContext] = getContextFromMessage(message)
    if (parentContext.isDefined) {
      Some(tracer.createSubcontext(parentContext.get))
    } else {
      None
    }
  }
}

object TraceHeader extends Enumeration {
  val TraceId = Value("X-TraceId")
  val SpanId = Value("X-SpanId")
  val ParentSpanId = Value("X-ParentSpanId")
}

