package com.wajam.nrv.cluster

import org.scalatest.FunSuite
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TestNode extends FunSuite {

  test("node as map key") {
    val n1 = new Node("127.0.0.1", Map("nrv" -> 1000, "test" -> 1001))
    val n1a = new Node("127.0.0.1", Map("nrv" -> 1000, "test" -> 1002))
    val n2 = new Node("127.0.0.1", Map("nrv" -> 1010, "test" -> 1011))
    val n2a = new Node("127.0.0.1", Map("nrv" -> 1010, "test" -> 1011))

    assert(n1 == n1a)
    assert(n2 == n2a)
    assert(n2 != n1)

    var m = Map[Node, Node]()
    m += (n2 -> n2)
    m += (n1 -> n1)
    assert(m(n2a) == n2)
    assert(m(n1a) == n1)
  }
}
