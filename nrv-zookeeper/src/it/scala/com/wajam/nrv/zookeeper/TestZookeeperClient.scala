package com.wajam.nrv.zookeeper

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.apache.zookeeper.CreateMode
import com.wajam.nrv.zookeeper.ZookeeperClient._
import org.scalatest.{BeforeAndAfter, FunSuite}
import com.wajam.nrv.utils.{Future, Promise}
import org.scalatest.matchers.ShouldMatchers._
import java.io.IOException
import com.yammer.metrics.scala.MetricsGroup

@RunWith(classOf[JUnitRunner])
class TestZookeeperClient extends FunSuite with BeforeAndAfter {
  var zClient: ZookeeperClient = null

  before {
    zClient = new ZookeeperClient("127.0.0.1")

    zClient.deleteRecursive("/tests")
    zClient.create("/tests", "testing", CreateMode.PERSISTENT)
  }

  after {
    if (zClient != null)
      zClient.close()
  }

  test("client should connect and change its state to connected") {
    assert(zClient.connected)
  }

  test("client should notify when connected") {
    zClient.close()

    val pConnected = Promise[Boolean]

    zClient = new ZookeeperClient("127.0.0.1", autoConnect = false)
    zClient.addObserver {

      case ZookeeperConnected(orEvent) =>
        pConnected.success(true)

      case _ =>
    }
    zClient.connect()

    Future.blocking(pConnected.future, 1000)

    assert(pConnected.future.isCompleted)
  }

  test("client should increment error counter when not able to connect to host name") {
    var connectErrorCounter = new MetricsGroup(classOf[ZookeeperClient]).counter("connect-error")
    connectErrorCounter.count should be(0)

    // Testing auto connect
    evaluating {
      new ZookeeperClient("unknownserveraddress")
    } should produce [IOException]

    connectErrorCounter.count should be(1)

    // Testing without auto connect
    val client = new ZookeeperClient("unknownserveraddress", autoConnect = false)
    evaluating {
      client.connect()
    } should produce [IOException]

    connectErrorCounter.count should be(2)
  }

  test("creating a node should create it (obviously...)") {
    zClient.create("/tests/cluster", null, CreateMode.EPHEMERAL)
    assert(zClient.exists("/tests/cluster"))
  }

  test("setting data on node should change value of it") {
    zClient.set("/tests", "data:test")
    assert("data:test" == zClient.getString("/tests"))
  }

  test("ensureExists should create but doesn't throw if already exists") {
    assert(zClient.ensureExists("/tests/ext", "", CreateMode.PERSISTENT))
    assert(!zClient.ensureExists("/tests/ext", "", CreateMode.PERSISTENT))
  }

  test("ensureAllExists should create all path components") {
    zClient.exists("/tests/parent") should  be(false)
    zClient.exists("/tests/parent/child") should  be(false)

    zClient.ensureAllExists("/tests/parent/child", "data", CreateMode.PERSISTENT) should be(true)
    zClient.ensureAllExists("/tests/parent/child", "", CreateMode.PERSISTENT) should be(false)

    zClient.get("/tests/parent") should  be("".getBytes)
    zClient.get("/tests/parent/child") should be("data".getBytes)
  }

  test("ensureAllExists should not fail with a single and more path components") {
    zClient.ensureAllExists("/", "", CreateMode.PERSISTENT)
    zClient.ensureAllExists("/tests", "", CreateMode.PERSISTENT)
    zClient.ensureAllExists("/tests/parent", "", CreateMode.PERSISTENT)
    zClient.ensureAllExists("/tests/parent/child", "", CreateMode.PERSISTENT)
    zClient.ensureAllExists("/tests/parent/child/child", "", CreateMode.PERSISTENT)
  }

  test("incrementing a counter should create it an increment it") {
    assert(zClient.incrementCounter("/tests/counter", 10, 4) == 14)
    assert(zClient.getInt("/tests/counter") == 14)

    val v = zClient.incrementCounter("/tests/counter", 1)
    assert(v == 15, v)
    assert(zClient.getInt("/tests/counter") == 15)
  }

  test("delete recursive should delete nodes recursively") {
    zClient.create("/tests/rec1", "", CreateMode.PERSISTENT)
    zClient.create("/tests/rec1/rec2", "", CreateMode.PERSISTENT)
    zClient.create("/tests/rec1/rec2/rec3", "", CreateMode.EPHEMERAL)
    zClient.create("/tests/rec1/rec4", "", CreateMode.PERSISTENT)
    zClient.deleteRecursive("/tests/rec1")

    assert(!zClient.exists("/tests/rec1"))
  }

  test("getChildren should list children") {
    zClient.create("/tests/chl1", "", CreateMode.PERSISTENT)
    zClient.create("/tests/chl1/chl2", "", CreateMode.PERSISTENT)
    zClient.create("/tests/chl1/chl3", "", CreateMode.PERSISTENT)
    zClient.create("/tests/chl1/chl4", "", CreateMode.PERSISTENT)
    zClient.create("/tests/chl1/chl5", "", CreateMode.PERSISTENT)

    val children = zClient.getChildren("/tests/chl1").sortBy(x => x)
    assert(children == Seq("chl2", "chl3", "chl4", "chl5"))
  }

  test("should be able to register a watch on a node be notified only once on it when node value changes") {
    zClient.create("/tests/getwatch", "value1", CreateMode.PERSISTENT)

    val p = Promise[Boolean]
    var nbNotif = 0
    assert(zClient.getString("/tests/getwatch", Some((event: NodeValueChanged) => {
      nbNotif += 1
    })) == "value1")

    zClient.set("/tests/getwatch", "value2")

    assert(zClient.getString("/tests/getwatch", Some((event: NodeValueChanged) => {
      nbNotif += 1
      p.success(true)
    })) == "value2")

    zClient.set("/tests/getwatch", "value3")

    Future.blocking(p.future, 1000)
    assert(nbNotif == 2)
  }

  test("should be able to register a watch on a node be notified only once on it when children change") {
    zClient.create("/tests/childwatch", "value1", CreateMode.PERSISTENT)

    val p = Promise[Boolean]
    var nbNotif = 0
    assert(zClient.getChildren("/tests/childwatch", Some((event: NodeChildrenChanged) => {
      nbNotif += 1
    })) == Seq())

    zClient.create("/tests/childwatch/chl1", "value2", CreateMode.PERSISTENT)

    assert(zClient.getChildren("/tests/childwatch", Some((event: NodeChildrenChanged) => {
      nbNotif += 1
    })) == Seq("chl1"))

    zClient.create("/tests/childwatch/chl2", "value3", CreateMode.PERSISTENT)

    assert(zClient.getChildren("/tests/childwatch", Some((event: NodeChildrenChanged) => {
      nbNotif += 1
    })) == Seq("chl1", "chl2"))
    zClient.delete("/tests/childwatch/chl2")

    assert(zClient.getChildren("/tests/childwatch", Some((event: NodeChildrenChanged) => {
      nbNotif += 1
      p.success(true)
    })) == Seq("chl1"))
    zClient.delete("/tests/childwatch/chl1")

    Future.blocking(p.future, 1000)
    assert(nbNotif == 4)
  }

  test("should be able to convert timestamp to and from string") {
    val currentTime = System.currentTimeMillis()
    val currentTimeString = timestamp2string(currentTime)
    string2timestamp(currentTimeString) should  be (currentTime)
  }

  test("same timestamp encoded in string with different timezones should be equals") {
    string2timestamp("2012-11-05T17:25:52.946-0500") should be (string2timestamp("2012-11-06T03:25:52.946+0500"))
  }
}
