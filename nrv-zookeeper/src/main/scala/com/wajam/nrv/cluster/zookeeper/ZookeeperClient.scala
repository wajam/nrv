package com.wajam.nrv.cluster.zookeeper

import com.wajam.nrv.Logging
import java.util.concurrent.CountDownLatch
import org.apache.zookeeper.Watcher.Event.KeeperState
import org.apache.zookeeper.ZooDefs.Ids
import org.apache.zookeeper._
import data.Stat
import org.apache.zookeeper.KeeperException.Code

object ZookeeperClient {
  implicit def string2bytes(value: String) = value.getBytes

  implicit def int2bytes(value: Int) = value.toString.getBytes

  implicit def long2bytes(value: Long) = value.toString.getBytes
}

class ZookeeperClient(servers: String, sessionTimeout: Int = 3000, basePath: String = "", watcher: Option[ZookeeperClient => Unit] = None) extends Logging {
  @volatile private var zk: ZooKeeper = null
  connect()

  import ZookeeperClient._

  def getHandle: ZooKeeper = zk

  /**
   * connect() attaches to the remote zookeeper and sets an instance variable.
   */
  private def connect() {
    val connectionLatch = new CountDownLatch(1)
    val assignLatch = new CountDownLatch(1)
    if (zk != null) {
      zk.close()
      zk = null
    }
    zk = new ZooKeeper(servers, sessionTimeout,
      new Watcher {
        def process(event: WatchedEvent) {
          sessionEvent(assignLatch, connectionLatch, event)
        }
      })
    assignLatch.countDown()
    log.info("Attempting to connect to zookeeper servers %s".format(servers))
    connectionLatch.await()
  }

  def close() {
    zk.close()
  }

  def sessionEvent(assignLatch: CountDownLatch, connectionLatch: CountDownLatch, event: WatchedEvent) {
    log.info("Zookeeper event: %s".format(event))
    assignLatch.await()
    event.getState match {
      case KeeperState.SyncConnected =>
        try {
          watcher.map(fn => fn(this))
        } catch {
          case e: Exception =>
            log.error("Exception during zookeeper connection established callback")
        }
        connectionLatch.countDown()

      case KeeperState.Expired =>
        // TODO: notify manager! probably need we are not reliable anymore, we need to resync
        // Session was expired; create a new zookeeper connection
        connect()

      case _ =>
        // Disconnected -- zookeeper library will handle reconnects
    }
  }

  /**
   * Create single path
   */
  def create(path: String, data: Array[Byte], createMode: CreateMode): String = {
    zk.create(path, data, Ids.OPEN_ACL_UNSAFE, createMode)
  }

  def ensureExists(path: String, data: Array[Byte]): Boolean = {
    try {
      this.create(path, data, CreateMode.PERSISTENT)
      return true

    } catch {
      case e: KeeperException =>
        if (e.code() == Code.NODEEXISTS) {
          return true
        }

        throw e
    }
  }

  def get(path: String, stat: Stat = null): Array[Byte] = {
    zk.getData(makeNodePath(path), false, stat)
  }

  def getString(path: String, stat: Stat = null): String = {
    new String(zk.getData(makeNodePath(path), false, stat))
  }

  def getInt(path: String, stat: Stat = null): Int = {
    getString(path, stat).toInt
  }

  def getLong(path: String, stat: Stat = null): Long = {
    getString(path, stat).toLong
  }

  def set(path: String, data: Array[Byte], version: Int = -1) {
    zk.setData(makeNodePath(path), data, version)
  }

  def delete(path: String, version: Int = -1) {
    zk.delete(makeNodePath(path), version)
  }

  /**
   * Mostly from: https://svn.apache.org/repos/asf/incubator/flume/branches/branch-0.9.5/flume-core/src/main/java/com/cloudera/flume/master/ZooKeeperCounter.java
   */
  def incrementCounter(path: String, by: Long = 1, initial: Long = 0): Long = {
    val stat = new Stat
    var current: Long = 0

    this.ensureExists(path, initial)

    while (true) {
      var data: Array[Byte] = null
      try {
        data = this.get(path, stat)
      } catch {
        case e: Exception =>
      }

      current = data match {
        case d: Array[Byte] =>
          new String(d).toLong
        case null =>
          initial
      }

      current += by

      try {
        this.set(path, current, stat.getVersion)
        return current
      } catch {
        case e: KeeperException.BadVersionException =>
          debug("Counter increment retry for {}", path)
      }
    }

    current
  }

  private def makeNodePath(path: String) = "%s/%s".format(basePath, path).replaceAll("//", "/")
}
