package com.wajam.nrv.consistency.replication

import com.wajam.nrv.data.Message
import com.wajam.commons.timestamp.Timestamp
import com.wajam.commons.Closable

/**
 * Replication source iterator that abstract the source of the transaction to replicate. Transactions are ordered by
 * timestamp.
 */
trait ReplicationSourceIterator extends Iterator[Option[Message]] with Closable {
  def start: Timestamp
  def end: Option[Timestamp]

  override def withFilter(p: (Option[Message]) => Boolean): ReplicationSourceIterator = {
    val outer = this
    val filtered = super.withFilter(p)

    new ReplicationSourceIterator{
      def start = outer.start
      def end = outer.end

      def hasNext = filtered.hasNext

      def next() = filtered.next()

      def close() {
        outer.close()
      }
    }
  }
}