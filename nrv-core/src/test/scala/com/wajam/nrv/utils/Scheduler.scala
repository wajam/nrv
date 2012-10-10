package com.wajam.nrv.utils

import actors.Actor
import java.util

/**
 * Scheduler that calls a callback after a given delay and then each period
 *
 * @param cb Callback to call
 * @param delay Delay for first message
 * @param period Period between each message
 * @return Scheduler
 */
class Scheduler(cb: () => Unit, delay: Long, period: Long) {
  private val timer = new util.Timer

  timer.scheduleAtFixedRate(new util.TimerTask {
    def run() {
      cb()
    }
  }, delay, period)

  def forceSchedule() {
    cb()
  }

  def cancel() {
    timer.cancel()
  }
}

trait Scheduled extends Actor {
  private var _scheduler: Scheduler = null

  protected def scheduledMessage: Any

  protected def scheduledBlockingMessage: Boolean = false

  protected def scheduledPeriod: Long

  protected def scheduledDelay: Long = scheduledPeriod

  override def start(): Actor = {
    this._scheduler = new Scheduler(() => {
      this.sendScheduledMessage()
    }, scheduledDelay, scheduledPeriod)

    super.start()
  }

  private def sendScheduledMessage() {
    if (scheduledBlockingMessage)
      this !? scheduledMessage
    else
      this ! scheduledMessage
  }

  protected def forceScheduled() = _scheduler.forceSchedule()

  protected def cancelScheduled() = _scheduler.cancel()

}

