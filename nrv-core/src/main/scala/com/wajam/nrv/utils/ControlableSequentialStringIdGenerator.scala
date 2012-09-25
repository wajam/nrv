package com.wajam.nrv.utils

/**
 * Trait generating string identified from a sequence of number. Every call to #createId increment the sequence.
 * The initial sequence start at zero. The sequence can be reset manually.
 * Not thread safe. Used for testing.
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
