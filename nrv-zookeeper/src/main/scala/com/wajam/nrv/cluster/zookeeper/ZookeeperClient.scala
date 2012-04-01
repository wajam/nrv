package com.wajam.nrv.cluster.zookeeper

import com.wajam.nrv.Logging
import java.util.concurrent.CountDownLatch
import org.apache.zookeeper.Watcher.Event.KeeperState
import org.apache.zookeeper.ZooDefs.Ids
import org.apache.zookeeper._

class ZookeeperClient(servers: String, sessionTimeout: Int, basePath : String,
                      watcher: Option[ZookeeperClient => Unit]) extends Logging {
  @volatile private var zk : ZooKeeper = null
  connect()

  def this(servers: String) = this(servers, 3000, "", None)

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
      new Watcher { def process(event : WatchedEvent) {
        sessionEvent(assignLatch, connectionLatch, event)
      }})
    assignLatch.countDown()
    log.info("Attempting to connect to zookeeper servers %s".format(servers))
    connectionLatch.await()
  }

  def close() {
    zk.close()
  }

  def sessionEvent(assignLatch: CountDownLatch, connectionLatch : CountDownLatch, event : WatchedEvent) {
    log.info("Zookeeper event: %s".format(event))
    assignLatch.await()
    event.getState match {
      case KeeperState.SyncConnected => {
        try {
          watcher.map(fn => fn(this))
        } catch {
          case e:Exception =>
            log.error("Exception during zookeeper connection established callback")
        }
        connectionLatch.countDown()
      }
      case KeeperState.Expired => {
        // Session was expired; create a new zookeeper connection
        connect()
      }
      case _ => // Disconnected -- zookeeper library will handle reconnects
    }
  }

  /***
   * Create single path
   */
  def create(path: String, data: Array[Byte], createMode: CreateMode): String = {
    zk.create(path, data, Ids.OPEN_ACL_UNSAFE, createMode)
  }

  def get(path: String): Array[Byte] = {
    zk.getData(makeNodePath(path), false, null)
  }

  def set(path: String, data: Array[Byte]) {
    zk.setData(makeNodePath(path), data, -1)
  }

  def delete(path: String) {
    zk.delete(makeNodePath(path), -1)
  }

  private def makeNodePath(path : String) = "%s/%s".format(basePath, path).replaceAll("//", "/")

}
