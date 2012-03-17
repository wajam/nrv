package com.appaquet.nrv

import org.slf4j.LoggerFactory

/**
 * Trait that add logging capability to a class
 */
trait Logging {
  val log = LoggerFactory.getLogger(getClass)

  def debug(msg: => String, params:AnyRef*) = if (log.isDebugEnabled) log.debug(msg, params)

  def info(msg: => String, params:AnyRef*) = if (log.isInfoEnabled) log.info(msg, params)

  def warn(msg: => String, params:AnyRef*) = if (log.isWarnEnabled) log.warn(msg, params)

  def error(msg: => String, params:AnyRef*) = if (log.isErrorEnabled) log.error(msg, params)

}
