package com.wajam.nrv.extension.json.test

import org.scalatest.Assertions
import net.liftweb.json.JsonAST.{JNothing, JValue}

/**
 *
 */
class JsonAssertions extends Assertions {

  def assertJsonEquivalent(expected: JValue, actual: JValue) {
    val difference = expected diff actual
    assert(difference.added === JNothing, "added")
    assert(difference.changed === JNothing, "changed")
    assert(difference.deleted === JNothing, "deleted")
  }

  def isJsonEquivalent(expected: JValue, actual: JValue) = {
    val difference = expected diff actual
    (difference.added == JNothing) &&
      (difference.changed == JNothing) &&
      (difference.deleted == JNothing)
  }

}
