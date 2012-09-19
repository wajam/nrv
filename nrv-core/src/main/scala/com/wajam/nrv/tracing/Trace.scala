package com.wajam.nrv.tracing

import java.text.SimpleDateFormat
import java.util.UUID
import java.net.InetSocketAddress
import com.wajam.nrv.tracing.Annotation.Message
import util.DynamicVariable
import com.wajam.nrv.utils.CurrentTime

/**
 *
 */
sealed trait Annotation
object Annotation {
  case class ClientSend() extends Annotation
  case class ClientRecv(code: Int) extends Annotation
  case class ServerSend(code: Int) extends Annotation
  case class ServerRecv() extends Annotation
  case class RpcName(service: String, path: String)  extends Annotation
  case class Message(content: String) extends Annotation
  case class ClientAddress(addr: InetSocketAddress) extends Annotation
  case class ServerAddress(addr: InetSocketAddress) extends Annotation
}

final case class TraceContext(traceId: Option[String] = TraceContext.createId, parentSpanId: Option[String] = None, spanId: Option[String] = TraceContext.createId) {

  if (traceId.isEmpty)
    throw new IllegalArgumentException("traceId must be specified")

  if (spanId.isEmpty)
    throw new IllegalArgumentException("spanId must be specified")
}

object TraceContext {
  def createId: Option[String] = {
    Some(UUID.randomUUID().toString)
  }
}

case class Record(context: TraceContext, timestamp: Long, annotation: Annotation, duration: Option[Long] = None) {

  override def toString = "<%s %s>, %s, %s".format(
    new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(timestamp), annotation, context, duration)
}

object Trace extends Trace {
}

protected[tracing] trait Trace extends CurrentTime {
  private val localContext: DynamicVariable[Option[TraceContext]] = new DynamicVariable[Option[TraceContext]](None)
  private val defaultTracer: Tracer = NullTracer

  def currentContext: Option[TraceContext] = {
    localContext.value
  }

  def currentTracer: Tracer = {
    defaultTracer
  }

  def trace[S](newContext: Option[TraceContext] = None)(block: => S): S = {

    val context: TraceContext = (currentContext, newContext) match {
      case (None, None) => TraceContext()
      case (cur, None) => TraceContext(currentContext.get.traceId, currentContext.get.spanId)
      case (None, ctx) => ctx.get
      case (cur, ctx) => validateContext(newContext.get)
    }

    localContext.withValue(Some(context)) {
      block
    }
  }

  def record(annotation: Annotation) {
    if (currentContext.isEmpty)
      throw new IllegalStateException("No trace context")

    currentTracer.record(Record(currentContext.get, currentTime, annotation, None))
  }

  def time[S](message: String)(block: => S) {

    if (currentContext.isEmpty)
      throw new IllegalStateException("No trace context")

    val start = currentTime
    try {
      block
    } finally {
      val end = currentTime
      currentTracer.record(Record(currentContext.get, end, Message(message), Some(end-start)))
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
