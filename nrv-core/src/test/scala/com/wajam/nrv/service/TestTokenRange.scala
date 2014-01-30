package com.wajam.nrv.service

import org.scalatest.FunSuite
import org.scalatest.Matchers._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TestTokenRange extends FunSuite {

  test("contains") {
    TokenRange(0, 1000).contains(0) should be(true)
    TokenRange(0, 1000).contains(500) should be(true)
    TokenRange(0, 1000).contains(1000) should be(true)
    TokenRange(0, 1000).contains(1001) should be(false)
    TokenRange(0, 1000).contains(-1) should be(false)
    TokenRange(0, 0).contains(0) should be(true)
  }

  test("next range") {
    val ranges = List(TokenRange(0, 1000), TokenRange(1001, 2000), TokenRange(2001, 3000))
    val clonedRanges = List() ++ ranges
    val emptyRanges = List()
    val otherRanges = List(TokenRange(5000, 6000))

    ranges(0).nextRange(ranges) should be(Some(ranges(1)))
    ranges(0).nextRange(clonedRanges) should be(Some(ranges(1)))
    ranges(0).nextRange(emptyRanges) should be(None)
    ranges(0).nextRange(otherRanges) should be(None)

    ranges(1).nextRange(ranges) should be(Some(ranges(2)))
    ranges(2).nextRange(ranges) should be(None)
  }
}
