package com.wajam.nrv.service

import org.scalatest.FunSuite
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TestRing extends FunSuite {
  val ring = new Object with Ring[Int];

  test("addition") {
    ring.add(1, 1);
    ring.add(4, 4);
    ring.add(5, 5);
    ring.add(8, 8);
    assert(ring.size == 4)
  }

  test("resolving") {
    var r = ring.resolve(4, 2)
    assert(r.size == 2)
    assert(r(0).token == 4, r(0) + " should equals 4")

    r = ring.resolve(9, 2)
    assert(r.size == 2)
    assert(r(0).token == 1, r(0) + " should equals 1")
  }

  test("delete") {
    ring.delete(5)
    assert(ring.size == 3)
  }
}
