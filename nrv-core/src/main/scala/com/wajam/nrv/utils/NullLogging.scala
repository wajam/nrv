package com.wajam.nrv.utils

import com.wajam.nrv.Logging

/**
 * Trait that prevent logging. Usefull to prevent logging in a class without removing the logging calls. Preferably
 * used only in test classes.
 */
trait NullLogging extends Logging {

  override def debug(msg: => String, params: Any*) {
  }

  override def trace(msg: => String, params: Any*) {
  }

  override def info(msg: => String, params: Any*) {
  }

  override def warn(msg: => String, params: Any*) {
  }

  override def error(msg: => String, params: Any*) {
  }
}
