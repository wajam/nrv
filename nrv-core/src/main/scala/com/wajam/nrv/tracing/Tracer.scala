package com.wajam.nrv.tracing

import java.text.SimpleDateFormat
import java.util.UUID
import java.net.InetSocketAddress
import com.wajam.nrv.tracing.Annotation.Message
import util.DynamicVariable
import com.wajam.nrv.utils.{UuidStringGenerator, IdGenerator, CurrentTime}

/**
 * Trace context information. All trace events initiated from a common ancestor call share the same TraceId.
 * Every outgoing and incomming messages are recorded in a new subcontext (i.e. new SpanId)
 * refering to its parent SpanIn. The root span has not parent SpanId.
 */
final case class TraceContext(traceId: Option[String], parentSpanId: Option[String], spanId: Option[String]) {

  if (traceId.isEmpty)
    throw new IllegalArgumentException("traceId must be specified")

  if (spanId.isEmpty)
    throw new IllegalArgumentException("spanId must be specified")
}

/**
 * Contextual trace information record. A record is an annotation with context and timestamp.
 */
case class Record(context: TraceContext, timestamp: Long, annotation: Annotation, duration: Option[Long] = None) {

  override def toString = "<%s %s>, %s, %s".format(
    new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(timestamp), annotation, context, duration)
}

/**
 * Basic trace event information without a context.
 */
sealed trait Annotation

object Annotation {

  case class ClientSend() extends Annotation

  case class ClientRecv(code: Option[Int]) extends Annotation

  case class ServerSend(code: Option[Int]) extends Annotation

  case class ServerRecv() extends Annotation

  case class RpcName(service: String, protocol: String, method: String, path: String) extends Annotation

  case class Message(content: String) extends Annotation

  case class ClientAddress(addr: InetSocketAddress) extends Annotation

  case class ServerAddress(addr: InetSocketAddress) extends Annotation

}

/**
 * The tracer is used to record traces. It maintain the current trace context.
 */
class Tracer(private val recorder: TraceRecorder = NullTraceRecorder,
             private val currentTimeGenerator: CurrentTime = new CurrentTime {},
             private val idGenerator: IdGenerator[String] = new UuidStringGenerator {}) {

  private val localContext: DynamicVariable[Option[TraceContext]] = new DynamicVariable[Option[TraceContext]](None)

  /**
   * Returns the current trace context. The current trace context is only valid if the caller is a block of code
   * executed from withing a #trace method call.
   */
  def currentContext: Option[TraceContext] = {
    localContext.value
  }

  /**
   * Create a new globally unique identified. Used to create a new TraceId or a new SpanId composing a trace context.
   */
  def createId: Option[String] = {
    Some(idGenerator.createId)
  }

  /**
   * Creates a new subcontext object from the specified context. Just create a new context object and does not affect
   * the current context.
   */
  def createChildContext(parent: TraceContext): TraceContext = {
    TraceContext(parent.traceId, parent.spanId, createId)
  }

  /**
   * Execute the specified block of code in the specified trace context which become the #currentContext. If a trace
   * context is specified, it must be a direct subcontext of the current contex. If there is no current context, the
   * specified context become the current context. If no trace context is specified, a new one is created as a
   * subcontext of the current context or as a new root context if there is no current context.
   */
  def trace[S](newContext: Option[TraceContext] = None)(block: => S) {

    val context: TraceContext = (currentContext, newContext) match {
      // No current or new context provided. Create a brand new one.
      case (None, None) => TraceContext(createId, None, createId)
      // No new context provided, use current context.
      case (cur, None) => createChildContext(currentContext.get)
      // No current context but one is provided, use provided context.
      case (None, ctx) => ctx.get
      // Both current context and new context provided, validate that the new context is a direct child.
      case (cur, ctx) => validateContext(newContext.get)
    }

    localContext.withValue(Some(context)) {
      block
    }
  }

  /**
   * Record a new trace annotation. Must be called from a code block executed within a #trace method call.
   */
  def record(annotation: Annotation) {
    if (currentContext.isEmpty)
      throw new IllegalStateException("No trace context")

    recorder.record(Record(currentContext.get, currentTimeGenerator.currentTime, annotation, None))
  }

  /**
   * Execute the specified code block and record a new message annotation with a the block execution duration once
   * executed. Must be called from a code block executed within a #trace method call.
   */
  def time[S](message: String)(block: => S) {

    if (currentContext.isEmpty)
      throw new IllegalStateException("No trace context")

    val start = currentTimeGenerator.currentTime
    try {
      block
    } finally {
      val end = currentTimeGenerator.currentTime
      recorder.record(Record(currentContext.get, end, Message(message), Some(end - start)))
    }
  }

  private def validateContext(child: TraceContext): TraceContext = {

    val parent: TraceContext = currentContext.get

    if (child.traceId != parent.traceId)
      throw new IllegalArgumentException("Child traceId [%s] does not match parent traceId [%s]".format(child.traceId, parent.traceId))
    Map
    if (child.parentSpanId != parent.spanId)
      throw new IllegalArgumentException("Child parentSpanId [%s] does not match parent spanId [%s]".format(child.parentSpanId, parent.spanId))

    if (child.spanId == parent.spanId)
      throw new IllegalArgumentException("Child spanId [%s] MUST not match parent spanId".format(child.spanId))

    child
  }
}
