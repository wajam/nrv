package com.wajam.nrv.consistency.replication

import com.wajam.nrv.consistency.{ConsistentStore, ResolvedServiceMember}
import com.yammer.metrics.scala.Instrumented
import com.wajam.nrv.utils.timestamp.Timestamp

/**
 * Replication source iterator backed by a ConsistentStore. The iterator is bounded i.e. the first timestamp and the
 * last timestamp are known.
 */
class ConsistentStoreReplicationIterator(member: ResolvedServiceMember, startTimestamp: Timestamp,
                                         endTimestamp: Timestamp, store: ConsistentStore)
  extends ReplicationSourceIterator with Instrumented {
  val start = startTimestamp
  val end = Some(endTimestamp)
  private val itr = store.readTransactions(startTimestamp, endTimestamp, member.ranges)
  private var lastTxTimestamp: Option[Long] = None

  def hasNext = itr.hasNext

  def next() = {
    val message = itr.next()
    lastTxTimestamp = message.timestamp.map(_.value)
    Some(message)
  }

  def close() {
    itr.close()
  }
}
