package com.wajam.nrv.consistency.replication

import com.wajam.nrv.consistency.{Consistency, ConsistentStore, ResolvedServiceMember}
import com.wajam.nrv.utils.timestamp.Timestamp
import com.yammer.metrics.scala.Instrumented
import com.wajam.nrv.data.Message

/**
 * Replication source iterator backed by a ConsistentStore. The iterator is bounded i.e. the first timestamp and the
 * last timestamp are known.
 */
class ConsistentStoreReplicationIterator(member: ResolvedServiceMember, fromTimestamp: Timestamp,
                                         toTimestamp: Timestamp, store: ConsistentStore)
  extends ReplicationSourceIterator with Instrumented
{
  val from = fromTimestamp
  val to = Some(toTimestamp)
  private val itr = store.readTransactions(fromTimestamp, toTimestamp, member.ranges)
  private var lastTxTimestamp: Option[Long] = None

  lazy private val nextMeter = metrics.meter("next", "next", member.scopeName)
  private val lastTransactionTimestampGauge = metrics.gauge("last-tx-timestamp", member.scopeName) {
    lastTxTimestamp.getOrElse(0L)
  }

  def hasNext = itr.hasNext

  def next() = {
    val message = itr.next()
    lastTxTimestamp = Consistency.getMessageTimestamp(message).map(_.value)
    Some(message)
  }

  def close() {
    itr.close()
  }
}
