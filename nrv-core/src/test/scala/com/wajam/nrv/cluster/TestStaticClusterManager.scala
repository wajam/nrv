package com.wajam.nrv.cluster

import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import com.wajam.nrv.service.Service

@RunWith(classOf[JUnitRunner])
class TestStaticClusterManager extends FunSuite {

  test("add member to service") {
    val node1 = new Node("127.0.0.1", Map("nrv" -> 12345))
    val node2 = new Node("127.0.0.2", Map("nrv" -> 12345))
    val node3 = new Node("127.0.0.3", Map("nrv" -> 12345))


    val clusterManager = new StaticClusterManager
    val cluster = new Cluster(node1, clusterManager)

    val service = new Service("test_service")
    cluster.registerService(service)

    clusterManager.addMember(service, 1000, node1)
    clusterManager.addMember(service, 5000, node2)
    clusterManager.addMember(service, 9000, node3)

    val t1 = service.resolveMembers(50, 1)
    assert(t1.length == 1)
    assert(t1(0).node == node1)

    val t2 = service.resolveMembers(2000, 1)
    assert(t2.length == 1)
    assert(t2(0).node == node2)

    val t3 = service.resolveMembers(7000, 1)
    assert(t3.length == 1)
    assert(t3(0).node == node3)

    val t4 = service.resolveMembers(10000, 1)
    assert(t4.length == 1)
    assert(t4(0).node == node1)
  }
}
