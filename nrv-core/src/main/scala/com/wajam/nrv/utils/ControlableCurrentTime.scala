package com.wajam.nrv.utils

/**
 *
 */
trait ControlableCurrentTime extends CurrentTime {

  protected var value: Long = System.currentTimeMillis()

  override def currentTime = value
  def currentTime_=(newValue: Long) {
    value = newValue
  }

  def advanceTime(delta: Long) {
    value += delta
  }
}
