package com.wajam.nrv.utils

import actors.Actor
import java.util

/**
 * Scheduler that sends a message to an actor at a regular interval.
 * @param actor Actor to send message to
 * @param message Message to send to actor
 * @param delay Delay for first message
 * @param period Period between each message
 * @param blocking Is the sent message waiting for a response
 * @return Scheduler
 */
class Scheduler(actor: Actor, message: Any, delay: Long, period: Long, blocking: Boolean = false) {
  private val timer = new util.Timer

  timer.scheduleAtFixedRate(new util.TimerTask {
    def run() {
      forceSchedule()
    }
  }, delay, period)

  def cancel() {
    timer.cancel()
  }

  def forceSchedule() {
    if (blocking)
      actor !? message
    else
      actor ! message
  }
}

object Scheduler {
  /**
   * Create a scheduler
   * @param actor Actor to send message to
   * @param message Message to send to actor
   * @param delay Delay for first message
   * @param period Period between each message
   * @param blocking Is the sent message waiting for a response
   * @return Scheduler
   */
  def schedule(actor: Actor, message: Any, delay: Long, period: Long, blocking: Boolean = false) =
    new Scheduler(actor, message, delay, period, blocking)
}

trait Scheduled extends Actor {
  private var _scheduler: Option[Scheduler] = None

  /**
   * Schedule periodic message to actor
   *
   * @param message Message to send to actor
   * @param delay Delay for first message
   * @param period Period between each message
   * @param blocking Is the sent message waiting for a response
   */
  protected def scheduleMessage(message: Any, delay: Long, period: Long, blocking: Boolean = false) {
    _scheduler match {
      case None => _scheduler = Some(new Scheduler(this, message, delay, period))
      case Some(scheduler) => throw new RuntimeException("This actor is already scheduled")
    }
  }

  protected def forceSchedule() = _scheduler match {
    case Some(scheduler) => scheduler.forceSchedule()
    case _ => // Do nothing
  }

  protected def cancelScheduler() {
    _scheduler match {
      case Some(scheduler) => scheduler.cancel()
      case _ => // Do nothing
    }
  }

}

