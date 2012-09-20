package com.wajam.nrv.service

import com.wajam.nrv.data.{Message, MessageType, InMessage, OutMessage}
import com.wajam.nrv.tracing.{TraceContext, TraceHeader, Tracer, Annotation}
import com.wajam.nrv.Logging
import java.net.{Inet4Address, NetworkInterface, InetAddress, InetSocketAddress}
import com.wajam.nrv.cluster.Node

/**
 *
 */
object TraceFilter extends MessageHandler with Logging {

  override def handleIncoming(action: Action, message: InMessage) {
    handleIncoming(action, message, _ => {})
  }

  override def handleIncoming(action: Action, message: InMessage, next: (Unit) => Unit) {

    message.function match {
      // Message is an incomming request. Inherit from received trace context or create a new one
      case MessageType.FUNCTION_CALL =>
        val traceContext = createChildContext(message).getOrElse(TraceContext())

        TraceHeader.clearContext(message.metadata) // Clear trace context metadata in request message
        action.tracer.trace(Some(traceContext)) {
          action.tracer.record(Annotation.ServerRecv())
          action.tracer.record(toRpcName(action, message))
          action.tracer.record(Annotation.ServerAddress(toInetSocketAddress(action, message)))
          next()
        }

      // Message is an incomming response. Use matching out message trace context
      case MessageType.FUNCTION_RESPONSE if message.matchingOutMessage.isDefined =>
        val traceContext: Option[TraceContext] = TraceHeader.getContext(message.matchingOutMessage.get.metadata)
        action.tracer.trace(traceContext) {
          action.tracer.record(Annotation.ClientRecv(message.code))
          next()
        }

      // TODO: Handle this gracefully!
//      case _ =>


    }
  }

  override def handleOutgoing(action: Action, message: OutMessage) {
    handleOutgoing(action, message, _ => {})
  }

  override def handleOutgoing(action: Action, message: OutMessage, next: (Unit) => Unit) {
    message.function match {
      // Message is a call to an external service. Create a sub context (i.e. new span) for the call
      case MessageType.FUNCTION_CALL =>
        val traceContext = createChildContext(message).getOrElse({
          // TODO: Fail with an exception once trace context propagation is integrated in all services
          debug("Outgoing request has not trace context! {}", toRpcName(action, message))
          TraceContext()
//          throw new IllegalStateException("Outgoing request has not trace context! " + toRpcName(message))
        })

        // Set trace context metadata in request message
        TraceHeader.setContext(message.metadata, Some(traceContext))

        action.tracer.trace(Some(traceContext)){
          action.tracer.record(Annotation.ClientSend())
          action.tracer.record(toRpcName(action, message))
          action.tracer.record(Annotation.ClientAddress(toInetSocketAddress(action, message)))
          next()
        }

      case _ =>
        // Message is a response for an external service. Already in an trace context.
        TraceHeader.clearContext(message.metadata) // Clear trace context metadata in response message

        // TODO: Fail with an exception once trace context propagation is integrated in all services
        if (action.tracer.currentContext.isEmpty)
          debug("Outgoing response has not trace context! {}", toRpcName(action, message))
//          throw new IllegalStateException("Outgoing response has not trace context! " + toRpcName(message))
        else
          action.tracer.record(Annotation.ServerSend(message.code))

        next()
    }
  }

  /**
   * Creates a new RpcName annotation from the specified message information
   */
  private def toRpcName(action: Action, message: Message): Annotation.RpcName = {
    Annotation.RpcName(message.serviceName, message.protocolName, message.method, action.path)
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
  private def createChildContext(message: Message): Option[TraceContext] = {
    val parentContext: Option[TraceContext] = TraceHeader.getContext(message.metadata)
    if (parentContext.isDefined) {
      Some(TraceContext(parentContext.get.traceId, parentContext.get.spanId))
    } else {
      None
    }
  }
}
