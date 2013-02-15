package com.wajam.nrv.consistency.persistence

import com.wajam.nrv.utils.timestamp.Timestamp

trait TransactionLog {
  /**
   * Returns the most recent timestamp written on the log storage.
   */
  def getLastLoggedTimestamp: Option[Timestamp]

  /**
   * Appends the specified transaction event to the transaction log
   */
  def append(tx: TransactionEvent)

  /**
   * Read all the transaction from the specified timestamp
   */
  def read(timestamp: Timestamp): TransactionLogIterator

  /**
   * Truncate log storage from the specified timestamp inclusively
   */
  def truncate(timestamp: Timestamp): Boolean

  /**
   * Ensure that transaction log is fully written on the log storage
   */
  def commit()

  /**
   * Close this transaction log
   */
  def close()
}

trait TransactionLogIterator extends Iterator[TransactionEvent] {
  def close()
}