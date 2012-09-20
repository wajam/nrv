package com.wajam.nrv.utils

/**
 *
 */
class ControlableSequencialStringIdGenerator extends IdGenerator[String] {
  var value: Int = 0

  def createId = {
    val id = value.toString
    value += 1
    id
  }

  def reset() { value = 0 }
}
