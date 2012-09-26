package com.wajam.nrv.tracing

import com.wajam.nrv.Logging

/**
 * Trait for recording trace events
 */
trait TraceRecorder {
  def record(record: Record)
}

/**
 * No op trace recorder which records nothing.
 */
object NullTraceRecorder extends TraceRecorder {
  def record(record: Record) {
    // No op
  }
}

/**
 * Record trace events with standard logging.
 */
object LoggingTraceRecorder extends TraceRecorder with Logging {
  def record(record: Record) {
    info(record.toString)
  }
}

/**
 * Print trace events at the console.
 */
object ConsoleTraceRecorder extends TraceRecorder {
  def record(record: Record) {
    println(record.toString)
  }
}



