package com.wajam.nrv.utils

import com.wajam.nrv.Logging

/**
 * Trait that add message filtering capability to logging
 */
trait MessageFilterLogging extends Logging {
  def filterLogMessage: (String, Seq[Any]) => (String, Seq[Any])

  override def debug(msg: => String, params: Any*) {
    if (log.isDebugEnabled) {
      val (filteredMsg, filteredParams) = filterLogMessage(msg, params)
      super.debug(filteredMsg, filteredParams)
    }
  }

  override def trace(msg: => String, params: Any*) {
    if (log.isTraceEnabled) {
      val (filteredMsg, filteredParams) = filterLogMessage(msg, params)
      super.trace(filteredMsg, filteredParams)
    }
  }

  override def info(msg: => String, params: Any*) {
    if (log.isInfoEnabled) {
      val (filteredMsg, filteredParams) = filterLogMessage(msg, params)
      super.info(filteredMsg, filteredParams)
    }
  }

  override def warn(msg: => String, params: Any*) {
    if (log.isWarnEnabled) {
      val (filteredMsg, filteredParams) = filterLogMessage(msg, params)
      super.warn(filteredMsg, filteredParams)
    }
  }

  override def error(msg: => String, params: Any*) {
    if (log.isErrorEnabled) {
      val (filteredMsg, filteredParams) = filterLogMessage(msg, params)
      super.error(filteredMsg, filteredParams)
    }
  }

}
