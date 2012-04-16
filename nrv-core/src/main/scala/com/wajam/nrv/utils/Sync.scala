package com.wajam.nrv.utils

/**
 * Async call synchronisation tool
 */
class Sync[T >:Null <:AnyRef](bubbleException:Boolean = true) {
  private var value:T = null
  private var err:Exception = null

  def error(ex:Exception) {
    this.done(null, ex)
  }

  def done(value:T = null, ex:Exception = null) {
    this.value = value
    this.err = ex

    this.synchronized {
      this.notify()
    }
  }

  def get(timeout:Long = 0):T = {
    this.synchronized {
      this.wait(timeout)
    }

    if (err != null && bubbleException)
      throw err

    value
  }


  def thenWait(cb: T=> Unit, timeout:Long = 0) {
    cb(this.get(timeout))
  }
}

object Sync {
  def empty = new Sync[AnyRef]
}
