package com.wajam.nrv.utils

/**
 * Generate unique long id based on the current time. Can generate up to 10K unique id if invoked in
 * the same millisecond. This class is not thread safe and must be invoked from a single thread, synchronized
 * externally or used with SynchronizedIdGenerator
 */
class TimestampIdGenerator extends IdGenerator[Long] with CurrentTime {

  private var seed: Long = currentTime
  private var seedIndex = 0

  def nextId = {
    val newSeed = currentTime
    if (newSeed == seed) {
      seedIndex += 1
    } else {
      seed = newSeed
      seedIndex = 0
    }

    if (seedIndex > 9999)
      throw new IndexOutOfBoundsException

    seed * 10000 + seedIndex
  }
}
