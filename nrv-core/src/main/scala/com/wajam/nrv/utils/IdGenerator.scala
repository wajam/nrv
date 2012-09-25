package com.wajam.nrv.utils

/**
 * Trait for creating identifier.
 */
trait IdGenerator[T] {
  def createId: T
}
