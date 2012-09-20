package com.wajam.nrv.tracing

import com.wajam.nrv.Logging

/**
 *
 */
trait TraceRecorder {
  def record(record: Record)
}

object LoggingTraceRecorder extends TraceRecorder with Logging {
  def record(record: Record) {
    info(record.toString)
  }
}

object ConsoleTraceRecorder extends TraceRecorder {
  def record(record: Record) {
    println(record.toString)
  }
}

object NullTraceRecorder extends TraceRecorder {
  def record(record: Record) {
    // No op
  }
}


