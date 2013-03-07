package com.wajam.nrv.consistency

import com.wajam.nrv.data.Message
import com.wajam.nrv.utils.timestamp.Timestamp
import com.wajam.nrv.service.TokenRange

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
   * Truncate all records at the given timestamp for the specified token.
   */
  def truncateAt(timestamp: Timestamp, token: Long)

  /**
   * Truncate all records from the given timestamp inclusively for the specified token ranges.
   */
  def truncateFrom(timestamp: Timestamp, tokens: Seq[TokenRange])
}

