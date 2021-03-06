package com.wajam.nrv.consistency.log

import com.wajam.nrv.consistency.log.LogRecord.Index
import com.wajam.nrv.utils.timestamp.Timestamp

object NullTransactionLog extends TransactionLog {

  def getLastLoggedRecord = None

  def append[T <: LogRecord](block: => T): T = {
    block
  }

  def read(index: Index) = EmptyTransactionLogIterator

  def read(timestamp: Timestamp) = EmptyTransactionLogIterator

  def truncate(index: Index) {}

  def commit() {}

  def close() {}
}

object EmptyTransactionLogIterator extends TransactionLogIterator {
  def hasNext = false

  def next() = null

  def close() {}
}

