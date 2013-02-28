package com.wajam.nrv.consistency.persistence

import com.wajam.nrv.utils.timestamp.Timestamp
import com.wajam.nrv.consistency.persistence.LogRecord.Index

object NullTransactionLog extends TransactionLog {

  def getLastLoggedIndex = None

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

