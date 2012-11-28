package com.wajam.nrv

/**
 * Exception thrown when a remote call has timeout
 */
class TimeoutException(msg:String, var elapsed: Option[Long] = None) extends Exception {

  override def getMessage = elapsed match {
    case Some(time) => "%s (%d ms)".format(msg, time)
    case _ => msg
  }
}
