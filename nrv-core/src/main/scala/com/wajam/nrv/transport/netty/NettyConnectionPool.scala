package com.wajam.nrv.transport.netty

import org.jboss.netty.channel.Channel
import java.util.concurrent.{ConcurrentLinkedDeque, ConcurrentHashMap}
import scala.collection.JavaConversions._
import java.util.concurrent.atomic.AtomicInteger
import java.net.InetSocketAddress
import com.yammer.metrics.scala.Instrumented
import com.yammer.metrics.core.Gauge

/**
 * This class...
 *
 * User: felix
 * Date: 13/04/12
 */
class NettyConnectionPool(timeout: Long, maxSize: Int) extends Instrumented {

  private val connectionMap = new ConcurrentHashMap[InetSocketAddress, ConcurrentLinkedDeque[(Channel, Long)]]()
  private val atomicInteger = new AtomicInteger(0)

  private val connectionPoolSizeGauge = metrics.metricsRegistry.newGauge(NettyConnectionPool.getClass,
    "connection-pool-size", new Gauge[Long] { def value() = connectionMap.size() })

  def poolConnection(destination: InetSocketAddress, connection: Channel): Boolean = {
    clean()
    if (!connection.isOpen) {
      return false
    }
    var queue = connectionMap.get(destination)
    if (queue == null) {
      val newQueue = new ConcurrentLinkedDeque[(Channel, Long)]()
      queue = connectionMap.putIfAbsent(destination, newQueue)
      if (queue == null) {
        queue = newQueue
      }
    }

    var added = false
    if (atomicInteger.incrementAndGet() <= maxSize) {
      added = queue.add((connection, getTime()))
    }
    if (!added) {
      atomicInteger.decrementAndGet()
    }
    added
  }

  def getPooledConnection(destination: InetSocketAddress): Option[Channel] = {
    clean()
    var queue = connectionMap.get(destination)
    if (queue == null) {
      return None
    }
    val connectionPoolEntry = queue.poll()
    atomicInteger.decrementAndGet()

    if (connectionPoolEntry != null) {
      Some(connectionPoolEntry._1)
    } else {
      None
    }
  }

  private def clean() {
    connectionMap.entrySet().foreach(e => cleanDeque(e.getValue()))
  }

  private def cleanDeque(deque: ConcurrentLinkedDeque[(Channel, Long)]) {
    deque.foreach(connectionPoolEntry => {
      if (!connectionPoolEntry._1.isOpen() ||
        (getTime() - connectionPoolEntry._2) >= timeout) {
        connectionPoolEntry._1.close()
        deque.remove(connectionPoolEntry)
      }
    })
  }

  protected def getTime() = {
    System.currentTimeMillis()
  }

}
object NettyConnectionPool  {

}
