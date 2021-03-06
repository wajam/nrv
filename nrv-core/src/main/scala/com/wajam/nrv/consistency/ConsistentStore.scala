package com.wajam.nrv.consistency

import com.wajam.nrv.data.Message
import com.wajam.nrv.service.TokenRange
import com.wajam.commons.Closable
import com.wajam.nrv.utils.timestamp.Timestamp

/**
 * Trait defining the API a consistent data store service must implement to be replicated within an NRV cluster
 */
trait ConsistentStore {

  /**
   * Force store cache invalidation.
   */
  def invalidateCache(): Unit

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
   * Setup the function which returns the most recent timestamp considered as consistent by the Consistency manager
   * for the specified token range. The consistency of the records more recent that the consistent timestamp is
   * unconfirmed and these records must be excluded from processing tasks such as GC or percolation.
   */
  def setCurrentConsistentTimestamp(getCurrentConsistentTimestamp: (TokenRange) => Timestamp)

  /**
   * Returns the mutation messages from and up to the given timestamps inclusively for the specified token ranges.
   */
  def readTransactions(fromTime: Timestamp, toTime: Timestamp, ranges: Seq[TokenRange]): Iterator[Message] with Closable

  /**
   * Apply the specified mutation message to this consistent database
   */
  def writeTransaction(message: Message)

  /**
   * Truncate all records at the given timestamp for the specified token.
   */
  def truncateAt(timestamp: Timestamp, token: Long)
}
