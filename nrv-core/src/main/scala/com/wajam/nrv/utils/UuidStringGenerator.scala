package com.wajam.nrv.utils

import java.util.UUID

/**
 *
 */
class UuidStringGenerator extends IdGenerator[String] {
  def createId = UUID.randomUUID().toString
}
