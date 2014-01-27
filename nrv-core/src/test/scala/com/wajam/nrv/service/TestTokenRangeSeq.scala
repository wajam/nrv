package com.wajam.nrv.service

import org.scalatest.FunSuite
import org.scalatest.Matchers._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TestTokenRangeSeq extends FunSuite {
  test("sequence head should returns the first range") {
    val seq = TokenRangeSeq(Seq(TokenRange(1, 5), TokenRange(11, 18)))
    seq.head should be(TokenRange(1, 5))
  }

  test("large chunk sequence head should returns the first range") {
    val seq = TokenRangeSeq(Seq(TokenRange(1, 5), TokenRange(11, 18)), chunkSize = 1000)
    seq.head should be(TokenRange(1, 5))
  }

  test("small chunk sequence head should returns the first chunk") {
    val seq = TokenRangeSeq(Seq(TokenRange(1, 5), TokenRange(11, 18)), chunkSize = 3)
    seq.head should be(TokenRange(1, 3))
  }

  test("sequence should find expected range") {
    val seq = TokenRangeSeq(Seq(TokenRange(1, 5), TokenRange(11, 18), TokenRange(20, 20)))
    seq.find(0L) should be(None)
    seq.find(1L) should be(Some(TokenRange(1, 5)))
    seq.find(3L) should be(Some(TokenRange(1, 5)))
    seq.find(4L) should be(Some(TokenRange(1, 5)))
    seq.find(5L) should be(Some(TokenRange(1, 5)))
    seq.find(6L) should be(None)
    seq.find(7L) should be(None)
    seq.find(10L) should be(None)
    seq.find(11L) should be(Some(TokenRange(11, 18)))
    seq.find(12L) should be(Some(TokenRange(11, 18)))
    seq.find(18L) should be(Some(TokenRange(11, 18)))
    seq.find(18L) should be(Some(TokenRange(11, 18)))
    seq.find(19L) should be(None)
    seq.find(20L) should be(Some(TokenRange(20, 20)))
    seq.find(21L) should be(None)
    seq.find(1000L) should be(None)
  }

  test("large chunk sequence should find expected range") {
    val seq = TokenRangeSeq(Seq(TokenRange(1, 5), TokenRange(11, 18), TokenRange(20, 20)), chunkSize = 1000)
    seq.find(0L) should be(None)
    seq.find(1L) should be(Some(TokenRange(1, 5)))
    seq.find(3L) should be(Some(TokenRange(1, 5)))
    seq.find(4L) should be(Some(TokenRange(1, 5)))
    seq.find(5L) should be(Some(TokenRange(1, 5)))
    seq.find(6L) should be(None)
    seq.find(7L) should be(None)
    seq.find(10L) should be(None)
    seq.find(11L) should be(Some(TokenRange(11, 18)))
    seq.find(12L) should be(Some(TokenRange(11, 18)))
    seq.find(18L) should be(Some(TokenRange(11, 18)))
    seq.find(18L) should be(Some(TokenRange(11, 18)))
    seq.find(19L) should be(None)
    seq.find(20L) should be(Some(TokenRange(20, 20)))
    seq.find(21L) should be(None)
    seq.find(1000L) should be(None)
  }

  test("small chunk sequence should find expected chunk") {
    val seq = TokenRangeSeq(Seq(TokenRange(1, 5), TokenRange(11, 18), TokenRange(20, 20)), chunkSize = 3)
    seq.find(0L) should be(None)
    seq.find(1L) should be(Some(TokenRange(1, 3)))
    seq.find(3L) should be(Some(TokenRange(1, 3)))
    seq.find(4L) should be(Some(TokenRange(4, 5)))
    seq.find(5L) should be(Some(TokenRange(4, 5)))
    seq.find(6L) should be(None)
    seq.find(7L) should be(None)
    seq.find(10L) should be(None)
    seq.find(11L) should be(Some(TokenRange(11, 13)))
    seq.find(12L) should be(Some(TokenRange(11, 13)))
    seq.find(13L) should be(Some(TokenRange(11, 13)))
    seq.find(14L) should be(Some(TokenRange(14, 16)))
    seq.find(15L) should be(Some(TokenRange(14, 16)))
    seq.find(16L) should be(Some(TokenRange(14, 16)))
    seq.find(17L) should be(Some(TokenRange(17, 18)))
    seq.find(18L) should be(Some(TokenRange(17, 18)))
    seq.find(19L) should be(None)
    seq.find(20L) should be(Some(TokenRange(20, 20)))
    seq.find(21L) should be(None)
    seq.find(1000L) should be(None)
  }

  test("sequence next should returns next range") {
    val seq = TokenRangeSeq(Seq(TokenRange(1, 5), TokenRange(11, 18), TokenRange(20, 20)))
    seq.next(TokenRange(0, 0)) should be(None)
    seq.next(TokenRange(1, 2)) should be(None)
    seq.next(TokenRange(1, 6)) should be(None)
    seq.next(TokenRange(2, 5)) should be(None)
    seq.next(TokenRange(1, 5)) should be(Some(TokenRange(11, 18)))
    seq.next(TokenRange(6, 18)) should be(None)
    seq.next(TokenRange(11, 18)) should be(Some(TokenRange(20, 20)))
    seq.next(TokenRange(20, 20)) should be(None)
  }

  test("large chunk sequence next should returns next range") {
    val seq = TokenRangeSeq(Seq(TokenRange(1, 5), TokenRange(11, 18), TokenRange(20, 20)), chunkSize = 1000)
    seq.next(TokenRange(0, 0)) should be(None)
    seq.next(TokenRange(1, 2)) should be(None)
    seq.next(TokenRange(1, 6)) should be(None)
    seq.next(TokenRange(2, 5)) should be(None)
    seq.next(TokenRange(1, 5)) should be(Some(TokenRange(11, 18)))
    seq.next(TokenRange(6, 18)) should be(None)
    seq.next(TokenRange(11, 18)) should be(Some(TokenRange(20, 20)))
    seq.next(TokenRange(20, 20)) should be(None)
  }

  test("small chunk sequence next should returns next chunk") {
    val seq = TokenRangeSeq(Seq(TokenRange(1, 5), TokenRange(11, 18), TokenRange(20, 20)), chunkSize = 3)
    seq.next(TokenRange(1, 5)) should be(None)
    seq.next(TokenRange(11, 18)) should be(None)
    seq.next(TokenRange(20, 20)) should be(None)

    seq.next(TokenRange(0, 0)) should be(None)
    seq.next(TokenRange(1, 2)) should be(None)
    seq.next(TokenRange(1, 3)) should be(Some(TokenRange(4, 5)))
    seq.next(TokenRange(4, 5)) should be(Some(TokenRange(11, 13)))
    seq.next(TokenRange(11, 13)) should be(Some(TokenRange(14, 16)))
    seq.next(TokenRange(14, 16)) should be(Some(TokenRange(17, 18)))
    seq.next(TokenRange(17, 18)) should be(Some(TokenRange(20, 20)))
  }

  test("sequence iterator should iterate over ranges") {
    val seq = TokenRangeSeq(Seq(TokenRange(1, 5), TokenRange(11, 18), TokenRange(20, 20)))
    seq.toIterator.toSeq should be(Seq(TokenRange(1, 5), TokenRange(11, 18), TokenRange(20, 20)))
  }

  test("large chunk sequence iterator should iterate over ranges") {
    val seq = TokenRangeSeq(Seq(TokenRange(1, 5), TokenRange(11, 18), TokenRange(20, 20)), chunkSize = 1000)
    seq.toIterator.toSeq should be(Seq(TokenRange(1, 5), TokenRange(11, 18), TokenRange(20, 20)))
  }

  test("small chunk sequence iterator should iterate over chunks") {
    val seq = TokenRangeSeq(Seq(TokenRange(1, 5), TokenRange(11, 18), TokenRange(20, 20)), chunkSize = 3)
    val expectedSeq = Seq(
      TokenRange(1, 3), TokenRange(4, 5),
      TokenRange(11, 13), TokenRange(14, 16), TokenRange(17, 18),
      TokenRange(20, 20))
    seq.toIterator.toSeq should be(expectedSeq)
  }
}