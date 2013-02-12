package com.wajam.nrv.scribe

import com.wajam.nrv.tracing.{TraceRecordFormatter, Record, TraceRecorder}

/**
 * Scribe trace recorder.
 */
class ScribeTraceRecorder(scribeHost: String, scribePort: Int, samplingRate: Int) extends TraceRecorder {
  private val scribeClient = new ScribeClient("traces", scribeHost, scribePort)
  scribeClient.start()

  def record(record: Record) {
    val sampled = record.context.sampled match {
      case Some(value) => value
      case None => record.context.traceId.hashCode % samplingRate == 0
    }

    if (sampled) {
      scribeClient.log(TraceRecordFormatter.record2TabSeparatedString(record))
    }
  }
}
