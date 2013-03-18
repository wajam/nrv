package com.wajam.nrv.consistency

import com.wajam.nrv.data.Message
import com.wajam.nrv.utils.timestamp.Timestamp
import com.wajam.nrv.service.TokenRange
import com.wajam.nrv.utils.Closable

/**
 * Trait defining the API a consistent data store service must implement to be replicated within an NRV cluster
 */
trait ConsistentStore {
  /**
   * Returns true of the specified message must be handled (e.g. timestamped, written in transaction log and
   * replicated) by the consistency manager.
   */
  def requiresConsistency(message: Message): Boolean

  /**
   * Returns the latest record timestamp for the specified token ranges
   */
  def getLastTimestamp(ranges: Seq[TokenRange]): Option[Timestamp]

  /**
   * Set the most recent timestamp considered as consistent from Consistency for the specified token ranges.
   * The consistency of the more recent records is unconfirmed and must not be included in processing tasks as
   * GC or percolation.
   */
  def setLastConsistentTimestamp(timestamp: Timestamp, ranges: Seq[TokenRange])

  /**
   * Returns the mutation messages from and up to the given timestamps inclusively for the specified token ranges.
   */
  def readTransactions(from: Timestamp, to: Timestamp, ranges: Seq[TokenRange]): Iterator[Message] with Closable

  /**
   * Apply the specified mutation message to this consistent database
   */
  def writeTransaction(message: Message)

  /**
   * Truncate all records at the given timestamp for the specified token.
   */
  def truncateAt(timestamp: Timestamp, token: Long)
}
