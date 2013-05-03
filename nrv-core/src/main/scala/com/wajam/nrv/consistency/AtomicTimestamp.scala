package com.wajam.nrv.consistency

import com.wajam.nrv.utils.timestamp.Timestamp
import AtomicTimestamp._
import java.util.concurrent.atomic.AtomicReference

/**
 * A timestamp that can be updated atomically according a predicate.
 */
class AtomicTimestamp(shouldUpdate: UpdatePredicate, initialTimestamp: Option[Timestamp] = None) {

  private val value = new AtomicReference[Option[Timestamp]](initialTimestamp)

  def get: Option[Timestamp] = value.get()

  /**
   * Update timestamp if the update predicate allows new timestamp to be set over the currently saved timestamp.
   * Since we need to be certain that the new timestamp is effectively the updated value, this method try to set
   * the value as long as either the new value was atomically set or is rejected by the predicate.
   */
  def update(timestamp: Option[Timestamp]) {
    var done = false
    do {
      val savedTimestamp = value.get()
      done = (savedTimestamp, timestamp) match {
        case (Some(prevTimestamp), Some(newTimestamp)) if shouldUpdate(prevTimestamp, newTimestamp) => {
          value.compareAndSet(savedTimestamp, timestamp)
        }
        case (Some(_), Some(_)) => true // Predicate is false, stop trying
        case (None, Some(_)) => value.compareAndSet(savedTimestamp, timestamp)
        case _ => true
      }
    } while (!done)
  }
}

object AtomicTimestamp {
  type UpdatePredicate = (Timestamp, Timestamp) => Boolean

  /**
   * Predicate used to update timestamp only if the new timestamp is greater than the previous value
   */
  def updateIfGreater(prevValue: Timestamp, newValue: Timestamp): Boolean = newValue > prevValue
}
