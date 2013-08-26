package com.wajam.nrv.scribe

import com.wajam.nrv.tracing.{TraceRecordFormatter, Record, TraceRecorder}

/**
 * Scribe trace recorder.
 */
class ScribeTraceRecorder(scribeHost: String, scribePort: Int, samplingRate: Int) extends TraceRecorder {
  private val scribeClient = new ScribeClient("traces", scribeHost, scribePort)
  scribeClient.start()

  def record(record: Record) {
    executeIfSampled(record) { record =>
      scribeClient.log(TraceRecordFormatter.record2TabSeparatedString(record))
    }
  }
}
