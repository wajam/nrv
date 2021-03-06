package com.wajam.nrv.cluster

import org.scalatest.FunSuite
import org.scalatest.Matchers._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import com.wajam.commons.InetUtils

@RunWith(classOf[JUnitRunner])
class TestNode extends FunSuite {

  test("node should have a toString and should be recreatable with fromString") {
    val n1 = new Node("127.0.0.1", Map("nrv" -> 1000, "test" -> 1001))
    val expected = "%s:nrv=1000,test=1001".format(InetUtils.firstInetAddress.get.getHostName)
    n1.toString should be(expected)
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
    val expectedKey = "%s_1000".format(InetUtils.firstInetAddress.get.getHostName)
    n2.uniqueKey should be(expectedKey)
  }

  test("node created with ip and host name are equals") {
    val n1 = new Node("localhost", Map("nrv" -> 1000))
    val n2 = new Node("127.0.0.1", Map("nrv" -> 1000))

    n1 should be(n2)
  }

  test("local node and node with same address should be equals") {
    val n1 = new LocalNode("127.0.0.1", Map("nrv" -> 1000))
    val n2 = new Node("127.0.0.1", Map("nrv" -> 1000))

    n1 should be(n2)
    n2 should be(n1)
  }


  test("local node allow dual loopback, and leave localIp alone") {
    val n1 = new LocalNode("127.1.1.1", Map("nrv" -> 1000))
    val n2 = new LocalNode("127.1.1.2", Map("nrv" -> 1000))

    def checkValid(ip: String, localNode: LocalNode) = {
      localNode.listenAddress.getHostAddress should be(ip)
      localNode.host.getHostAddress should be(ip)

      localNode.listenAddress.getHostName should be(ip)
      localNode.host.getHostName should be(ip)

      localNode.host.isLoopbackAddress should be(true)
      localNode.listenAddress.isLoopbackAddress should be(true)
    }

    checkValid("127.1.1.1", n1)
    checkValid("127.1.1.2", n2)
  }

  test("local node any local address and should resolve hostname") {
    val n1 = new LocalNode("0.0.0.0", Map("nrv" -> 1000))

    n1.listenAddress.isAnyLocalAddress should be(true)
    n1.host.isAnyLocalAddress should be(false)

    n1.listenAddress.isLoopbackAddress should be(false)
  }

  test("local node with IP containing numbers higher than 127") {
    val n = new LocalNode("128.128.128.128", Map("nrv" -> 1000))

    assert(n.address === "128.128.128.128")
  }
}
