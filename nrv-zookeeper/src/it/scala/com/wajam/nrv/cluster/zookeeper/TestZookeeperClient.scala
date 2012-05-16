package com.wajam.nrv.cluster.zookeeper

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.apache.zookeeper.{CreateMode, ZooKeeper}
import com.wajam.nrv.cluster.zookeeper.ZookeeperClient._
import org.scalatest.FunSuite

@RunWith(classOf[JUnitRunner])
class TestZookeeperClient extends FunSuite {
  var zClient = new ZookeeperClient("127.0.0.1")

  test("connected") {
    assert(zClient.getHandle.getState == ZooKeeper.States.CONNECTED)
  }

  test("create path") {
    try {
      zClient.delete("/cluster")
    } catch {
      case _ =>
    }

    zClient.create("/cluster", null, CreateMode.EPHEMERAL)

    // if version >= 0 then the path exists
    assert(zClient.getHandle.exists("/cluster", false).getVersion >= 0)
  }

  test("set data") {
    zClient.set("/cluster", "data:test")
    assert("data:test".equals(new String(zClient.getHandle.getData("/cluster", false, null))))
  }

  test("increment") {
    assert(zClient.incrementCounter("/counter", 10, 4) == 14)
    assert(zClient.getInt("/counter") == 14)
  }
}
