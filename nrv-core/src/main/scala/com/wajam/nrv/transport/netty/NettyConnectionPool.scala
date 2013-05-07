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
  private val currentNbPooledConnections = new AtomicInteger(0)

  private val poolHitMeter = metrics.meter("connection-pool-hit", "hits")
  private val poolMissMeter = metrics.meter("connection-pool-miss", "misses")
  private val poolAddsMeter = metrics.meter("connection-pool-adds", "additions")
  private val poolRemovesMeter = metrics.meter("connection-pool-removes", "removals")
  private val connectionPooledDestinationsGauge = metrics.gauge("connection-pooled-destinations-size") {
    connectionMap.size()
  }
  private val connectionPoolSizeGauge = metrics.gauge("connection-pool-size") {
    currentNbPooledConnections.longValue()
  }

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
    if (currentNbPooledConnections.incrementAndGet() <= maxSize) {
      added = queue.add((connection, getTime()))
      poolAddsMeter.mark()
    }
    if (!added) {
      currentNbPooledConnections.decrementAndGet()
    }
    added
  }

  def getPooledConnection(destination: InetSocketAddress): Option[Channel] = {
    clean()
    val connection = Option(connectionMap.get(destination)) flatMap (queue => {
      val connectionPoolEntry = Option(queue.poll())

      connectionPoolEntry map (_._1)
    })
    connection match {
      case Some(_) => {
        poolHitMeter.mark()
        currentNbPooledConnections.decrementAndGet()
      }
      case None => poolMissMeter.mark()
    }
    connection
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
        poolRemovesMeter.mark()
      }
    })
  }

  protected def getTime() = {
    System.currentTimeMillis()
  }

}
