package com.wajam.nrv.utils

import java.util.UUID

/**
 * Trait generating string representation of UUID identifier. This implementation is thread safe.
 */
trait UuidStringGenerator extends IdGenerator[String] {
  def nextId = UUID.randomUUID().toString
}
