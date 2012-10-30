package com.wajam.nrv.cluster

import org.scalatest.FunSuite
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TestNode extends FunSuite {

  test("node should have a toString and should be recreatable with fromString") {
    val n1 = new Node("127.0.0.1", Map("nrv" -> 1000, "test" -> 1001))
    assert(n1.toString == "localhost:nrv=1000,test=1001", n1.toString)
    val n2 = Node.fromString(n1.toString)
    assert(n1 == n2)
  }

  test("node can be used as a key of a map") {
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


  test("a unique string should be generated for each node and should resolve to node host if an ip is used") {
    val n1 = new Node("www.google.com", Map("nrv" -> 1000))
    assert(n1.uniqueKey == "www.google.com_1000", n1.uniqueKey)

    val n2 = new Node("127.0.0.1", Map("nrv" -> 1000))
    assert(n2.uniqueKey == "localhost_1000", n2.uniqueKey)
  }
}
