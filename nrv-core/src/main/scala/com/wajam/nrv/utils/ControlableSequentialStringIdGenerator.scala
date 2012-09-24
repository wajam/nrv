package com.wajam.nrv.utils

/**
 * Trait generating string identified from a sequence of number. Every call to #createId increment the sequence.
 * The initial sequence start at zero.
 * Not thread safe, The sequence can be reset. Used for testing.
 */
trait ControlableSequentialStringIdGenerator extends IdGenerator[String] {
  var value: Int = 0

  def createId = {
    val id = value.toString
    value += 1
    id
  }

  def reset() {
    value = 0
  }
}
