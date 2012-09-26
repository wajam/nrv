package com.wajam.nrv.utils

/**
 * Current Time trait that return current time in millisec (UTC Timestamp)
 */
trait CurrentTime {

  def currentTime: Long = System.currentTimeMillis()

}
