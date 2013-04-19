package com.wajam.nrv

import org.slf4j.LoggerFactory

/**
 * Trait that add logging capability to a class
 */
trait Logging {
  val innerLog = LoggerFactory.getLogger(getClass)

  @deprecated("Use methods already exposed by trait Logging")
  def log = this

  def debug(msg: => String, params: Any*) {
    if (innerLog.isDebugEnabled) {
      val arr = params.map(m => m.asInstanceOf[Object]).toArray
      innerLog.debug(msg, arr)
    }
  }

  def trace(msg: => String, params: Any*) {
    if (innerLog.isTraceEnabled) {
      val arr = params.map(m => m.asInstanceOf[Object]).toArray
      innerLog.trace(msg, arr)
    }
  }

  def info(msg: => String, params: Any*) {
    if (innerLog.isInfoEnabled) {
      val arr = params.map(m => m.asInstanceOf[Object]).toArray
      innerLog.info(msg, arr)
    }
  }

  def warn(msg: => String, params: Any*) {
    if (innerLog.isWarnEnabled) {
      val arr = params.map(m => m.asInstanceOf[Object]).toArray
      innerLog.warn(msg, arr)
    }
  }

  def error(msg: => String, params: Any*) {
    if (innerLog.isErrorEnabled) {
      val arr = params.map(m => m.asInstanceOf[Object]).toArray
      innerLog.error(msg, arr)
    }
  }

}
