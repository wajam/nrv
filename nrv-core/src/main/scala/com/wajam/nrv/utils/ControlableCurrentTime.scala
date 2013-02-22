package com.wajam.nrv.utils

/**
 * Trait specializing CurrentTime which always returns the same time as #currentTime (default is the trait creation)
 * unless explicitly modified. Used for testing.
 */
trait ControlableCurrentTime extends CurrentTime {

  protected var value: Long = System.currentTimeMillis()

  override def currentTime = value

  def currentTime_=(newValue: => Long) {
    value = newValue
  }

  def advanceTime(delta: Long) {
    value += delta
  }
}
