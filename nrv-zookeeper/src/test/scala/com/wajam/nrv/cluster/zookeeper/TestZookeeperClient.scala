package com.wajam.nrv.cluster.zookeeper

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.apache.zookeeper.{CreateMode, ZooKeeper}
import org.scalatest.{BeforeAndAfter, FunSuite}

@RunWith(classOf[JUnitRunner])
class TestZookeeperClient extends FunSuite {
  var zClient = new ZookeeperClient("127.0.0.1:13370")

  test("connected") {
    assert(zClient.getHandle.getState == ZooKeeper.States.CONNECTED)
  }

  test("create path") {
    zClient.delete("/cluster")
    zClient.create("/cluster", null, CreateMode.EPHEMERAL)

    // if version >= 0 then the path exists
    assert(zClient.getHandle.exists("/cluster", false).getVersion >= 0)
  }

  test("set data") {
    var data = new Array[Byte](1)
    val str = "data:test"
    data :+ str.toByte

    // zClient.create("/cluster", null, CreateMode.EPHEMERAL)
    zClient.set("/cluster", data)

    assert(zClient.getHandle.getData("/cluster", false, null) == data)
  }

  test("get data") {

  }
}
