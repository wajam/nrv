package com.wajam.nrv.cluster

import org.scalatest.FunSuite

class TestNode extends FunSuite {

  test("node as map key") {
    val n1 = new Node("127.0.0.1", Map("nrv" -> 1000, "test" -> 1001));
    val n1a = new Node("127.0.0.1", Map("nrv" -> 1000, "test" -> 1002));
    val n2 = new Node("127.0.0.1", Map("nrv" -> 1010, "test" -> 1011));
    val n2a = new Node("127.0.0.1", Map("nrv" -> 1010, "test" -> 1011));

    assert(n1 != n1a)
    assert(n2 == n2a)

    var m = Map[Node, Boolean]()
    m += (n2 -> true)

    assert(m(n2a) == true)
  }
}
