package com.wajam.nrv.utils

import java.util
import actors.Actor
import util.concurrent.atomic.AtomicBoolean

/**
 * Scheduler that calls a callback or send message to an actor after a given delay and then each period
 */
class Scheduler(cb: () => Unit, delay: Long, period: Long, autoStart: Boolean) {
  private val started = new AtomicBoolean()
  private val timer = new util.Timer

  def this(cb: () => Unit, delay: Long, period: Long) = this(cb, delay, period, true)

  def this(actor: Actor, message: AnyRef, delay: Long, period: Long, blockingMessage: Boolean, autoStart: Boolean) = this(() => {
    if (blockingMessage)
      actor !? message
    else
      actor ! message

  }, delay, period, autoStart)

  def this(actor: Actor, message: AnyRef, delay: Long, period: Long, blockingMessage: Boolean) =
    this(actor, message, delay, period, blockingMessage, true)


  if (autoStart) {
    start()
  }

  def start() {
    if (started.compareAndSet(false, true)) {
      timer.scheduleAtFixedRate(new util.TimerTask {
        def run() {
          cb()
        }
      }, delay, period)
    }
  }

  def forceSchedule() {
    cb()
  }

  def cancel() {
    if (started.compareAndSet(true, false)) {
      timer.cancel()
    }
  }
}
