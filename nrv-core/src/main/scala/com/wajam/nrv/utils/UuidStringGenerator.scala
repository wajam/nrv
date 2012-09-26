package com.wajam.nrv.utils

import java.util.UUID

/**
 * Trait generating string representation of UUID identifier.
 */
trait UuidStringGenerator extends IdGenerator[String] {
  def createId = UUID.randomUUID().toString
}
