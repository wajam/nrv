package com.wajam.nrv.transport

import org.jboss.netty.channel.Channel
import java.util.concurrent.{ConcurrentLinkedDeque, ConcurrentHashMap}
import scala.collection.JavaConversions._

/**
 * This class...
 *
 * User: felix
 * Date: 13/04/12
 */

class ConnectionPool(timeout: Long, maxSize: Int) {

  val connectionMap = new ConcurrentHashMap[String, ConcurrentLinkedDeque[ConnectionPoolEntry]]()

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

    queue.add(new ConnectionPoolEntry(connection, System.currentTimeMillis()))
  }

  def getPooledConnection(uri: String): Option[Channel] = {
    clean()
    var queue = connectionMap.get(uri)
    if (queue == null) {
      return None
    }
    Some(queue.poll().getChannel())
  }

  class ConnectionPoolEntry(channel: Channel, timestamp: Long) {
    def getChannel() = this.channel
    def getTimestamp() = this.timestamp
  }

  private def clean() {
    connectionMap.entrySet().foreach(e => cleanDeque(e.getValue()))
  }

  private def cleanDeque(deque: ConcurrentLinkedDeque) {
//    deque.foreach(connectionPoolEntry => {
//      if ((System.currentTimeMillis() - connectionPoolEntry.getTimestamp()) > )
//    })
  }

}
