package com.wajam.nrv.utils.timestamp

import org.scalatest.FunSuite
import org.scalatest.matchers.ShouldMatchers._
import scala.collection.immutable.TreeSet
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TestTimestamp extends FunSuite {

  test("should fail if sequence value is out of bounds") {
    evaluating {
      Timestamp(System.currentTimeMillis(), 10000)
    } should produce[IndexOutOfBoundsException]

    evaluating {
      Timestamp(System.currentTimeMillis(), -1)
    } should produce[IndexOutOfBoundsException]

    Timestamp(System.currentTimeMillis(), 0)
  }

  test("timeMs and seq should be preserved") {
    val expectedTimeMs = System.currentTimeMillis()
    val expectedSeq = 55
    val ts1 = Timestamp(expectedTimeMs, expectedSeq)
    val ts2 = Timestamp(ts1.value)
    val ts3 = Timestamp(ts2)
    
    ts1 should be(ts2)
    ts1 should be(ts3)
    ts2 should be(ts3)
    
    for (ts <- Seq(ts1, ts2, ts3)) {
      ts.timeMs should be(expectedTimeMs)
      ts.seq should be(expectedSeq)
    }
  }

  test("should be ordered") {
    val ts1 = Timestamp(1)
    val ts2 = Timestamp(2)
    val ts3 = Timestamp(3)

    ts1.compareTo(Timestamp(ts1)) should be(0)
    ts1.compareTo(ts2) should be < 0
    ts1.compareTo(ts3) should be < 0

    ts2.compareTo(ts1) should be > 0
    ts2.compareTo(Timestamp(ts2)) should be(0)
    ts2.compareTo(ts3) should be < 0

    ts3.compareTo(ts1) should be > 0
    ts3.compareTo(ts2) should be > 0
    ts3.compareTo(Timestamp(ts3)) should be(0)

    TreeSet(ts3, ts1, ts2).toList should be(List(ts1, ts2, ts3))
  }
}
