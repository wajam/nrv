package com.wajam.nrv.consistency.persistence

import com.wajam.nrv.utils.timestamp.Timestamp

object NullTransactionLog extends TransactionLog {
  def getLastLoggedTimestamp = None

  def append(tx: TransactionEvent) {}

  def read(timestamp: Timestamp) = NullTransactionLogIterator

  def truncate(timestamp: Timestamp) = true

  def commit() {}

  def close() {}

  object NullTransactionLogIterator extends TransactionLogIterator {
    def hasNext = false

    def next() = null

    def close() {}
  }

}
