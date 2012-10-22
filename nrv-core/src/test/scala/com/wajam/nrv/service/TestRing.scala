package com.wajam.nrv.service

import org.scalatest.FunSuite
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TestRing extends FunSuite {
  test("should add nodes") {
    val ring = new Object with Ring[Int];
    ring.add(1, 1);
    ring.add(4, 4);
    ring.add(5, 5);
    ring.add(8, 8);
    assert(ring.size == 4)
  }

  test("ring should be able to find a specific node") {
    val ring = new Object with Ring[Int];
    ring.add(1, 2);
    ring.add(4, 5);
    ring.add(5, 6);

    val node = ring.find(5) match {
      case None => fail("Couldn't find element with token 5")
      case Some(ringnode) => ringnode
    }

    assert(node.token == 5)
    assert(node.element == 6)
  }

  test("should resolve and returns given count") {
    val ring = new Object with Ring[Int];
    ring.add(1, 1);
    ring.add(4, 4);
    ring.add(5, 5);
    ring.add(8, 8);

    var r = ring.resolve(4, 2)
    assert(r.size == 2)
    assert(r(0).token == 4, r(0) + " should equals 4")

    r = ring.resolve(9, 2)
    assert(r.size == 2)
    assert(r(0).token == 1, r(0) + " should equals 1")
  }

  test("should resolve and returns given count using filter") {
    val ring = new Object with Ring[Int];
    ring.add(1, 1);
    ring.add(4, 4);
    ring.add(5, 5);
    ring.add(8, 8);

    var r = ring.resolve(4, 2, node => {
      node.token != 5
    })

    assert(r.size == 2)
    assert(r(0).token == 4, r(0) + " should equals 4")
    assert(r(1).token == 8, r(1) + " should equals 8")

    r = ring.resolve(5, 2, node => {
      node.token != 8
    })

    assert(r.size == 2)
    assert(r(0).token == 5, r(0) + " should equals 5")
    assert(r(1).token == 1, r(1) + " should equals 1")
  }

  test("should delete nodes") {
    val ring = new Object with Ring[Int];
    ring.add(1, 1);
    ring.add(4, 4);
    ring.add(5, 5);
    ring.add(8, 8);

    ring.delete(5)
    assert(ring.size == 3)
  }
}
