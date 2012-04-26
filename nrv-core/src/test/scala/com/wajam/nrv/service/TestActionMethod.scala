package com.wajam.nrv.service

import org.scalatest.FunSuite
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/**
 *
 */

@RunWith(classOf[JUnitRunner])
class TestActionMethod extends FunSuite {

  test("action method should not be case sensitive") {
    assert(ActionMethod("test").equals(ActionMethod("TEST")))
    assert(ActionMethod("test").matchMethod("TEST"))
  }

  test("ANY should always match") {
    assert(ActionMethod("test").matchMethod(ActionMethod.ANY))
    assert(ActionMethod(ActionMethod.ANY).matchMethod("TEST"))
  }
}
