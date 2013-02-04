package com.wajam.nrv.consistency

import com.wajam.nrv.data.Message

/**
 * Trait defining the API a consistent data store service must implement to be replicated within an NRV cluster
 */
trait ConsistentStore {
  /**
   * Returns true of the specified message must be handled (e.g. timestamped, written in transaction log and
   * replicated) by the consistency manager.
   */
  def requiresConsistency(message: Message): Boolean
}

