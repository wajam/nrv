package com.wajam.nrv.tracing

import com.wajam.nrv.Logging

/**
 *
 */
trait Tracer {
  def record(record: Record)
}

object LoggingTracer extends Tracer with Logging {
  def record(record: Record) {
    info(record.toString)
  }
}

object ConsoleTracer extends Tracer {
  def record(record: Record) {
    println(record.toString)
  }
}

object NullTracer extends Tracer {
  def record(record: Record) {
    // No op
  }
}


