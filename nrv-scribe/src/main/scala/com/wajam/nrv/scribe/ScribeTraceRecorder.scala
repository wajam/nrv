package com.wajam.nrv.scribe

import com.wajam.tracing.{TraceRecordFormatter, Record, TraceRecorder}

/**
 * Scribe trace recorder.
 */
class ScribeTraceRecorder(scribeHost: String, scribePort: Int) extends TraceRecorder {
  private val scribeClient = new ScribeClient("traces", scribeHost, scribePort)
  scribeClient.start()

  def record(record: Record) {
    scribeClient.log(TraceRecordFormatter.record2TabSeparatedString(record))
  }
}
