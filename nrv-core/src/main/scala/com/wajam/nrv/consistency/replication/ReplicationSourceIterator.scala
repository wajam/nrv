package com.wajam.nrv.consistency.replication

import com.wajam.nrv.data.Message
import com.wajam.nrv.utils.Closable
import com.wajam.nrv.utils.timestamp.Timestamp

/**
 * Replication source iterator that abstract the source of the transaction to replicate. Transactions are ordered by
 * timestamp.
 */
trait ReplicationSourceIterator extends Iterator[Option[Message]] with Closable {
  def from: Timestamp
  def to: Option[Timestamp]
}