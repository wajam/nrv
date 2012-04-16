package com.wajam.nrv.utils

/**
 * Async call synchronisation tool
 */
class Sync[T >:Null <:AnyRef](bubbleException:Boolean = true) {
  private var value:T = null
  private var err:Option[Exception] = None

  def error(ex:Exception) {
    this.done(null, Some(ex))
  }

  def done(value:T = null, ex:Option[Exception] = None) {
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

    err match {
      case Some(exception) => {
        if (bubbleException)
          throw err.get
      }
      case None =>
    }

    value
  }


  def thenWait(cb: T=> Unit, timeout:Long = 0) {
    cb(this.get(timeout))
  }
}

object Sync {
  def empty = new Sync[AnyRef]
}
