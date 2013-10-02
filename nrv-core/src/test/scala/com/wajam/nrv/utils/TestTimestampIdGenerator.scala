package com.wajam.nrv.utils

import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.matchers.ShouldMatchers._
import org.junit.runner.RunWith
import com.wajam.commons.ControlableCurrentTime

@RunWith(classOf[JUnitRunner])
class TestTimestampIdGenerator extends FunSuite {
  test("id should be unique and increase") {

    val generator = new TimestampIdGenerator
    val ids = new Array[Long](10000)
    for (i <- 0 until 10000) {
      ids(i) = generator.nextId
    }

    ids should be(ids.distinct.sorted)
  }

  test("should fail if invoked more than 10000 times in the same millisecond") {
    val generator = new TimestampIdGenerator with ControlableCurrentTime
    var lastId = 0L
    for (i <- 0 until 10000) {
      lastId = generator.nextId
    }

    evaluating {
      generator.nextId
    } should produce[IndexOutOfBoundsException]

    generator.advanceTime(1)

    generator.nextId should be(lastId + 1)
  }
}
