package com.wajam.nrv

import org.slf4j.LoggerFactory

/**
 * Trait that add logging capability to a class
 */
trait Logging {
  val log = LoggerFactory.getLogger(getClass)

  def debug(msg: => String, params: Any*) {
    if (log.isDebugEnabled) {
      val arr = params.map(m => m.asInstanceOf[Object]).toArray
      log.debug(msg, arr)
    }
  }

  def trace(msg: => String, params: Any*) {
    if (log.isTraceEnabled) {
      val arr = params.map(m => m.asInstanceOf[Object]).toArray
      log.trace(msg, arr)
    }
  }

  def info(msg: => String, params: Any*) {
    if (log.isInfoEnabled) {
      val arr = params.map(m => m.asInstanceOf[Object]).toArray
      log.info(msg, arr)
    }
  }

  def warn(msg: => String, params: Any*) {
    if (log.isWarnEnabled) {
      val arr = params.map(m => m.asInstanceOf[Object]).toArray
      log.warn(msg, arr)
    }
  }

  def error(msg: => String, params: Any*) {
    if (log.isErrorEnabled) {
      val arr = params.map(m => m.asInstanceOf[Object]).toArray
      log.error(msg, arr)
    }
  }

}
