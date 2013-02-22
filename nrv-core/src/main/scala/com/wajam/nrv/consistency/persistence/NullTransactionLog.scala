package com.wajam.nrv.consistency.persistence

import com.wajam.nrv.utils.timestamp.Timestamp
import com.wajam.nrv.consistency.persistence.LogRecord.Index

object NullTransactionLog extends TransactionLog {

  def getLastLoggedIndex = None

  def append[T <: LogRecord](block: => T): T = {
    block
  }

  def read(id: Option[Long], consistentTimestamp: Option[Timestamp]) = NullTransactionLogIterator

  def truncate(index: Index) = true

  def commit() {}

  def close() {}

  object NullTransactionLogIterator extends TransactionLogIterator {
    def hasNext = false

    def next() = null

    def close() {}
  }

}
