package com.wajam.nrv.consistency.persistence

import com.wajam.nrv.utils.timestamp.Timestamp
import com.wajam.nrv.consistency.persistence.LogRecord.Index

trait TransactionLog {
  /**
   * Returns the most recent consistant timestamp written on the log storage.
   */
  def getLastLoggedRecord: Option[LogRecord]

  /**
   * Appends the specified record to the transaction log
   */
  def append[T <: LogRecord](block: => T): T

  /**
   * Read all the records from the specified id, consistent timestamp or both
   */
  def read(id: Option[Long] = None, consistentTimestamp: Option[Timestamp] = None): TransactionLogIterator

  def read(index: Index): TransactionLogIterator = {
    read(Some(index.id), None)
  }

  def read(timestamp: Timestamp): TransactionLogIterator = {
    read(None, Some(timestamp))
  }

  /**
   * Truncate log storage from the specified index inclusively
   */
  def truncate(index: Index)

  /**
   * Ensure that transaction log is fully written on the log storage
   */
  def commit()

  /**
   * Close this transaction log
   */
  def close()
}

trait TransactionLogIterator extends Iterator[LogRecord] {
  def close()
}