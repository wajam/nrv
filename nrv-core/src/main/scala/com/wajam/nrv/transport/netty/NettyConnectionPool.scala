package com.wajam.nrv.transport.netty

import org.jboss.netty.channel.Channel
import java.util.concurrent.{ConcurrentLinkedDeque, ConcurrentHashMap}
import scala.collection.JavaConversions._
import java.util.concurrent.atomic.AtomicInteger

/**
 * This class...
 *
 * User: felix
 * Date: 13/04/12
 */

class NettyConnectionPool(timeout: Long, maxSize: Int) {

  private val connectionMap = new ConcurrentHashMap[String, ConcurrentLinkedDeque[ConnectionPoolEntry]]()
  private val atomicInteger = new AtomicInteger(0)

  def poolConnection(uri: String, connection: Channel): Boolean = {
    clean()
    var queue = connectionMap.get(uri)
    if (queue == null) {
      val newQueue = new ConcurrentLinkedDeque[ConnectionPoolEntry]()
      queue = connectionMap.putIfAbsent(uri, newQueue)
      if (queue == null) {
        queue = newQueue
      }
    }

    var added = false
    if (atomicInteger.incrementAndGet() <= maxSize) {
      added = queue.add(new ConnectionPoolEntry(connection, getTime()))
    }
    if (!added) {
      atomicInteger.decrementAndGet()
    }
    added
  }

  def getPooledConnection(uri: String): Option[Channel] = {
    clean()
    var queue = connectionMap.get(uri)
    if (queue == null) {
      return None
    }
    val channelEntry = queue.poll()
    atomicInteger.decrementAndGet()

    if (channelEntry != null) {
      Some(channelEntry.channel)
    } else {
      None
    }
  }

  class ConnectionPoolEntry(var channel: Channel, var timestamp: Long)

  private def clean() {
    connectionMap.entrySet().foreach(e => cleanDeque(e.getValue()))
  }

  private def cleanDeque(deque: ConcurrentLinkedDeque[ConnectionPoolEntry]) {
    deque.foreach(connectionPoolEntry => {
      if ((getTime() - connectionPoolEntry.timestamp) >= timeout) {
        connectionPoolEntry.channel.close()
        deque.remove(connectionPoolEntry)
      }
    })
  }

  protected def getTime() = {
    System.currentTimeMillis()
  }

}
