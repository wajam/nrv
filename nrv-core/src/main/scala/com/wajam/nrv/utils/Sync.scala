package com.wajam.nrv.utils

/**
 * Async call synchronisation tool
 */
class Sync[T >:Null <:AnyRef](bubbleException:Boolean = true) {
  var value:T = null
  var err:Exception = null

  def error(ex:Exception) {
    this.send(null, ex)
  }

  def send(value:T = null, ex:Exception = null) {
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

    if (err != null)
      throw err

    value
  }

  def then(cb: T=> Unit, timeout:Long = 0) {
    cb(this.get(timeout))
  }
}

object Sync {
  def empty = new Sync[AnyRef]
}
