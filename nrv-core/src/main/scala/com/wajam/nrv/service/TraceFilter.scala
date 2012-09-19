package com.wajam.nrv.service

import com.wajam.nrv.data.{Message, MessageType, InMessage, OutMessage}
import com.wajam.nrv.tracing.{TraceContext, TraceHeader, Trace, Annotation}
import com.wajam.nrv.Logging

/**
 *
 */
class TraceFilter extends MessageHandler with Logging {
  override def handleIncoming(action: Action, message: InMessage) {
    handleIncoming(action, message, _ => {})
  }

  override def handleIncoming(action: Action, message: InMessage, next: (Unit) => Unit) {

    message.function match {
      // Message is an incomming request. Inherit from received trace context or create a new one
      case MessageType.FUNCTION_CALL =>
        val traceContext = createChildContext(message).getOrElse(TraceContext())

        TraceHeader.clearContext(message.metadata) // Clear trace context metadata in request message
        Trace.trace(Some(traceContext)) {
          Trace.record(Annotation.ServerRecv())
          Trace.record(toRpcName(message))
          next()
        }

      // Message is an incomming response. Use matching out message trace context
      case MessageType.FUNCTION_RESPONSE if message.matchingOutMessage.isDefined =>
        val traceContext: Option[TraceContext] = TraceHeader.getContext(message.matchingOutMessage.get.metadata)
        Trace.trace(traceContext) {
          Trace.record(Annotation.ClientRecv(message.code))
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
          debug("Outgoing request has not trace context! {}", toRpcName(message))
          TraceContext()
//          throw new IllegalStateException("Outgoing request has not trace context! " + toRpcName(message))
        })

        // Set trace context metadata in request message
        TraceHeader.setContext(message.metadata, Some(traceContext))

        Trace.trace(Some(traceContext)){
          Trace.record(Annotation.ClientSend())
          Trace.record(toRpcName(message))
          next()
        }

      case _ =>
        // Message is a response for an external service. Already in an trace context.
        TraceHeader.clearContext(message.metadata) // Clear trace context metadata in response message

        // TODO: Fail with an exception once trace context propagation is integrated in all services
        if (Trace.currentContext.isEmpty)
          debug("Outgoing response has not trace context! {}", toRpcName(message))
//          throw new IllegalStateException("Outgoing response has not trace context! " + toRpcName(message))
        else
          Trace.record(Annotation.ServerSend(message.code))

        next()
    }
  }

  /**
   * Creates a new RpcName annotation from the specified message information
   */
  private def toRpcName(message: Message): Annotation.RpcName = {
    Annotation.RpcName(message.serviceName, message.protocolName + " " + message.method + " " + message.path)
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
