package com.wajam.nrv.tracing

/**
 *
 */
object TraceHeader extends Enumeration
{
  val TraceId = Value("X-TraceId")
  val SpanId = Value("X-SpanId")
  val ParentSpanId = Value("X-ParentSpanId")

  def getContext(map: scala.collection.mutable.Map[String, Any]): Option[TraceContext] = {
    val traceId: Option[String] = getValue(map, TraceId.toString)
    val spanId: Option[String] = getValue(map, SpanId.toString)
    val parentSpanId: Option[String] = getValue(map, ParentSpanId.toString)

    if (traceId.isDefined && spanId.isDefined)
      Some(TraceContext(traceId, parentSpanId, spanId))
    else
      None
  }

  def getValue(map: scala.collection.mutable.Map[String, Any], key: String): Option[String] = {
    map.getOrElse(key, null) match {
      case None => None
      case values: Seq[_] if values.isEmpty => None
      case values: Seq[_] => Some(values(0).toString)
      case value: String => Some(value)
      case _ => None
    }
  }

  def setContext(map: scala.collection.mutable.Map[String, Any], context: TraceContext) {
    clearContext(map)
    map(TraceId.toString) = context.traceId.get
    map(SpanId.toString) = context.spanId.get
    if (context.parentSpanId.isDefined)
      map(ParentSpanId.toString) = context.parentSpanId
  }

  def clearContext(map: scala.collection.mutable.Map[String, Any]) {
    values.foreach(map -= _.toString)
  }
}
