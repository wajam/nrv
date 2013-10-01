package com.wajam.nrv.utils.timestamp

import com.wajam.nrv.utils.timestamp.Timestamp

/**
 * Trait abstracting timestamp generation
 */
trait TimestampGenerator {

  /**
   * Fetch timestamps
   *
   * @param name timestamp sequence name
   * @param cb callback to call once the timestamp are assigned
   * @param nb the number of requested timestamps
   * @param token the originator message token. Callbacks with the same token are garanteed to be executed
   *              sequentialy. If sequentiality is not desired, specify -1 and the callback execution thread will be
   *              selected randomly
   */
  def fetchTimestamps(name: String, cb: (Seq[Timestamp], Option[Exception]) => Unit, nb: Int, token: Long)

  def responseTimeout: Long
}
