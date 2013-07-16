package com.wajam.nrv.utils

import java.util
import scala.actors.Actor
import util.concurrent.atomic.AtomicBoolean

/**
 * Scheduler that calls a callback or send message to an actor after a given delay and then each period
 */
class Scheduler(cb: () => Unit, delay: Long, period: Long, autoStart: Boolean, name: Option[String]) {
  private val started = new AtomicBoolean()
  private val timer = name match {
    case Some(timerName) => new util.Timer(timerName)
    case None => new util.Timer()
  }

  def this(cb: () => Unit, delay: Long, period: Long, name: Option[String]) = this(cb, delay, period, true, name)

  def this(actor: Actor, message: AnyRef, delay: Long, period: Long, blockingMessage: Boolean,
           autoStart: Boolean, name: Option[String]) = this(() => {
    if (blockingMessage)
      actor !? message
    else
      actor ! message

  }, delay, period, autoStart, name)

  def this(actor: Actor, message: AnyRef, delay: Long, period: Long, blockingMessage: Boolean, name: Option[String]) =
    this(actor, message, delay, period, blockingMessage, true, name)

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
