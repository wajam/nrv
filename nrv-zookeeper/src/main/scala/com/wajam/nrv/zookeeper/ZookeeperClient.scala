package com.wajam.nrv.zookeeper

import com.wajam.nrv.Logging
import java.util.concurrent.{Callable, CountDownLatch}
import org.apache.zookeeper.Watcher.Event.KeeperState
import org.apache.zookeeper.ZooDefs.Ids
import org.apache.zookeeper._
import org.apache.zookeeper.data.Stat
import org.apache.zookeeper.KeeperException.{NoNodeException, Code}
import scala.collection.JavaConversions._
import com.yammer.metrics.scala.Instrumented
import com.wajam.nrv.utils.{Event, Observable}
import java.text.SimpleDateFormat
import com.google.common.cache.CacheBuilder

object ZookeeperClient {
  implicit def string2bytes(value: String) = value.getBytes

  implicit def int2bytes(value: Int) = value.toString.getBytes

  implicit def long2bytes(value: Long) = value.toString.getBytes

  def timestampFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ")

  def timestamp2string(value: Long) = timestampFormat.format(value)

  def string2timestamp(value: String) = timestampFormat.parse(value).getTime

  // observable events
  case class ZookeeperConnected(originalEvent: WatchedEvent) extends Event

  case class ZookeeperDisconnected(originalEvent: WatchedEvent) extends Event

  case class ZookeeperExpired(originalEvent: WatchedEvent) extends Event

  case class NodeChildrenChanged(path: String, originalEvent: WatchedEvent) extends Event

  case class NodeValueChanged(path: String, originalEvent: WatchedEvent) extends Event

}

/**
 * Zookeeper client wrapper that wraps around zookeeper default client, implements more advanced
 * operation and give a Scala zest to the client.
 */
class ZookeeperClient(servers: String, sessionTimeout: Int = 3000, autoConnect: Boolean = true)
  extends Logging with Instrumented with Observable {

  import ZookeeperClient._

  @volatile private var zk: ZooKeeper = null

  // Caches watch function Watcher wrapper object using weak reference. Each watch function instance is garanteed to be
  // wrapped by a single Watcher object at a single time even in case of multiple registerations.
  // This result in a single change event per path/function instance.
  //
  // A Watcher object wrapper can be GC after the corresponding change event is fired because it is cached using weak
  // reference. It cannot be GC before because ZooKeeper keeps a hard reference to the Watcher object.
  private val dataWatches = CacheBuilder.newBuilder().weakValues.build[Function1[NodeValueChanged, Unit], Watcher]
  private val childWatches = CacheBuilder.newBuilder().weakValues.build[Function1[NodeChildrenChanged, Unit], Watcher]

  // metrics
  private lazy val metricsGetChildren = metrics.timer("get-children")
  private lazy val metricsGet = metrics.timer("get")
  private lazy val metricsCreate = metrics.timer("create")
  private lazy val metricsSet = metrics.timer("set")
  private lazy val metricsDelete = metrics.timer("delete")
  private lazy val metricsIncrement = metrics.timer("increment")
  private val metricsConnectError = metrics.counter("connect-error")

  if (autoConnect)
    this.connect()

  def getHandle: ZooKeeper = zk

  def connect() {
    val connectionLatch = new CountDownLatch(1)
    val assignLatch = new CountDownLatch(1)

    try {
      this.close()
      zk = new ZooKeeper(servers, sessionTimeout,
        new Watcher {
          def process(event: WatchedEvent) {
            sessionEvent(assignLatch, connectionLatch, event)
          }
        })
      assignLatch.countDown()
      log.info("Attempting to connect to zookeeper servers %s".format(servers))
      connectionLatch.await()
    } catch {
      case e: Exception =>
        // Increment by 2 to work around the stupid nagios check_jmx limitation
        // which prevent to set a critical value of 1
        metricsConnectError += 2
        throw e
    }
  }

  def close() {
    if (zk != null) {
      zk.close()
    }
  }

  def connected = zk != null && zk.getState.isConnected

  private def sessionEvent(assignLatch: CountDownLatch, connectionLatch: CountDownLatch, event: WatchedEvent) {
    log.info("Zookeeper event: %s".format(event))
    assignLatch.await()
    event.getState match {
      case KeeperState.SyncConnected =>
        connectionLatch.countDown()
        this.notifyObservers(new ZookeeperConnected(event))

      case KeeperState.Disconnected =>
        this.notifyObservers(new ZookeeperDisconnected(event))

      case KeeperState.Expired =>
        this.notifyObservers(new ZookeeperExpired(event))

        // Session was expired; create a new zookeeper connection
        this.connect()

      case other =>
        log.warn("Got a non-supported event in global event watcher: {}", event)
    }
  }

  def create(path: String, data: Array[Byte], createMode: CreateMode): String = {
    this.metricsCreate.time {
      zk.create(path, data, Ids.OPEN_ACL_UNSAFE, createMode)
    }
  }

  def ensureExists(path: String, data: Array[Byte], createMode: CreateMode = CreateMode.PERSISTENT): Boolean = {
    try {
      this.create(path, data, createMode)
      true

    } catch {
      case e: KeeperException =>
        if (e.code() == Code.NODEEXISTS) {
          return false
        }

        throw e
    }
  }

  def ensureAllExists(path: String, data: Array[Byte], createMode: CreateMode = CreateMode.PERSISTENT): Boolean = {
    val splitPath = path.split("/")
    if (splitPath.size > 1) {
      splitPath.slice(0, splitPath.size - 1).reduceLeft((parent, component) => {
        val path = "%s/%s".format(parent, component)
        ensureExists(path, Array(), createMode)
        path
      })
    }

    ensureExists(path, data, createMode)
  }

  def exists(path: String): Boolean = {
    val stat = zk.exists(path, false)
    stat != null && stat.getVersion >= 0
  }

  def getChildren(path: String, watch: Option[(NodeChildrenChanged) => Unit] = None, stat: Option[Stat] = None): Seq[String] = {
    this.metricsGetChildren.time {
      watch match {
        case Some(cb) => {
          val watcher: Watcher = childWatches.get(cb, new Callable[Watcher] {
            def call() = new Watcher {
              def process(event: WatchedEvent) {
                try {
                  cb(new NodeChildrenChanged(event.getPath, event))
                } catch {
                  case e: Exception => warn("Got an exception calling children watcher callback: {}", e)
                }
              }
            }
          })
          zk.getChildren(path, watcher, stat.getOrElse(null))
        }

        case None => {
          zk.getChildren(path, false, stat.getOrElse(null))
        }
      }
    }
  }

  def get(path: String, watch: Option[(NodeValueChanged) => Unit] = None, stat: Option[Stat] = None): Array[Byte] = {
    this.metricsGet.time {
      watch match {
        case Some(cb) => {
          val watcher: Watcher = dataWatches.get(cb, new Callable[Watcher] {
            def call() = new Watcher {
              def process(event: WatchedEvent) {
                try {
                  cb(new NodeValueChanged(event.getPath, event))
                } catch {
                  case e: Exception => warn("Got an exception calling watcher callback: {}", e)
                }
              }
            }
          })
          zk.getData(path, watcher, stat.getOrElse(null))
        }
        case None => {
          zk.getData(path, false, stat.getOrElse(null))
        }
      }
    }
  }

  def getString(path: String, watch: Option[(NodeValueChanged) => Unit] = None, stat: Option[Stat] = None): String = new String(get(path, watch, stat))

  def getInt(path: String, watch: Option[(NodeValueChanged) => Unit] = None, stat: Option[Stat] = None): Int = getString(path, watch, stat).toInt

  def getLong(path: String, watch: Option[(NodeValueChanged) => Unit] = None, stat: Option[Stat] = None): Long = getString(path, watch, stat).toLong

  def set(path: String, data: Array[Byte], version: Int = -1) {
    this.metricsSet.time {
      zk.setData(path, data, version)
    }
  }

  def delete(path: String) {
    this.metricsDelete.time {
      zk.delete(path, -1)
    }
  }

  def deleteRecursive(path: String) {
    def deleteCallback(curPath: String) {
      try {
        getChildren(curPath).foreach(childName => {
          val childPath = curPath + "/" + childName
          deleteCallback(childPath)
        })
        delete(curPath)
      } catch {
        case e: NoNodeException => /* nothing to see, keep moving on */
      }
    }
    deleteCallback(path)
  }

  /**
   * Mostly from: https://svn.apache.org/repos/asf/incubator/flume/branches/branch-0.9.5/flume-core/src/main/java/com/cloudera/flume/master/ZooKeeperCounter.java
   */
  def incrementCounter(path: String, by: Long = 1, initial: Long = 0): Long = {
    val stat = new Stat
    var current: Long = 0

    this.metricsIncrement.time {
      this.ensureExists(path, initial)

      while (true) {
        var data: Array[Byte] = null
        try {
          data = this.get(path, stat = Some(stat))
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
    }

    current
  }
}
