package com.wajam.nrv.utils

import com.wajam.nrv.Logging

/**
 * Trait that add log message transformation capability to logging
 */
trait TransformLogging extends Logging {
  def transformLogMessage: (String, Seq[Any]) => (String, Seq[Any])

  override def debug(msg: => String, params: Any*) {
    if (log.isDebugEnabled) {
      val (transformedMsg, transformedParams) = transformLogMessage(msg, params)
      super.debug(transformedMsg, transformedParams)
    }
  }

  override def trace(msg: => String, params: Any*) {
    if (log.isTraceEnabled) {
      val (transformedMsg, transformedParams) = transformLogMessage(msg, params)
      super.trace(transformedMsg, transformedParams)
    }
  }

  override def info(msg: => String, params: Any*) {
    if (log.isInfoEnabled) {
      val (transformedMsg, transformedParams) = transformLogMessage(msg, params)
      super.info(transformedMsg, transformedParams)
    }
  }

  override def warn(msg: => String, params: Any*) {
    if (log.isWarnEnabled) {
      val (transformedMsg, transformedParams) = transformLogMessage(msg, params)
      super.warn(transformedMsg, transformedParams)
    }
  }

  override def error(msg: => String, params: Any*) {
    if (log.isErrorEnabled) {
      val (transformedMsg, transformedParams) = transformLogMessage(msg, params)
      super.error(transformedMsg, transformedParams)
    }
  }

}
