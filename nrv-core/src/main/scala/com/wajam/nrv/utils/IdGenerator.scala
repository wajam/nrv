package com.wajam.nrv.utils

/**
 *
 */
trait IdGenerator[T] {
  def createId: T
}
