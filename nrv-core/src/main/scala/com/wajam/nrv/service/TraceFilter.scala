package com.wajam.nrv.service

import com.wajam.nrv.data.{Message, MessageType, InMessage, OutMessage}
import com.wajam.nrv.tracing.{TraceContext, TraceHeader, Trace, Annotation}

/**
 *
 */
class TraceFilter extends MessageHandler {
  override def handleIncoming(action: Action, message: InMessage) {
    handleIncoming(action, message, _ => {})
  }

  override def handleIncoming(action: Action, inMessage: InMessage, next: (Unit) => Unit) {

    inMessage.function match {
      // Message is an incomming request. Inherit from received trace context or create a new one
      case MessageType.FUNCTION_CALL =>
        val parentContext: Option[TraceContext] = TraceHeader.getContext(inMessage.metadata)
        val traceContext = if (parentContext.isDefined) {
          TraceContext(parentContext.get.traceId, parentContext.get.spanId)
        } else {
          TraceContext()
        }

        TraceHeader.clearContext(inMessage.metadata) // Clear trace context metadata in request message
        Trace.trace(Some(traceContext)) {
          Trace.record(Annotation.ServerRecv())
          Trace.record(toName(inMessage))
          next()
        }

      // Message is an incomming response. Use matching out message trace context
      case MessageType.FUNCTION_RESPONSE if inMessage.matchingOutMessage.isDefined =>
        val traceContext: Option[TraceContext] = TraceHeader.getContext(inMessage.matchingOutMessage.get.metadata)
        Trace.trace(traceContext) {
          Trace.record(Annotation.ClientRecv())
          next()
        }

      // TODO: Handle this gracefully!
//      case _ =>


    }
  }

  private def toName(message: Message): Annotation.Name = {
    Annotation.Name(message.serviceName, message.protocolName + " " + message.method + " " + message.path)
  }

  override def handleOutgoing(action: Action, message: OutMessage) {
    handleOutgoing(action, message, _ => {})
  }

  override def handleOutgoing(action: Action, outMessage: OutMessage, next: (Unit) => Unit) {
    outMessage.function match {
      // Message is a call to an external service. Create a sub context (i.e. new span) for the call
      case MessageType.FUNCTION_CALL =>
        val parentContext = TraceHeader.getContext(outMessage.metadata)
        val traceContext = Some(TraceContext(parentContext.get.traceId, parentContext.get.spanId))

        // Set trace context metadata in request message
        TraceHeader.setContext(outMessage.metadata, traceContext)

        Trace.trace(traceContext){
          Trace.record(Annotation.ClientSend())
          Trace.record(toName(outMessage))
          next()
        }

      case _ =>
        // Message is a response for an external service. Already in an trace context.
        TraceHeader.clearContext(outMessage.metadata) // Clear trace context metadata in response message
        Trace.record(Annotation.ServerSend())
        next()
    }
  }
}
