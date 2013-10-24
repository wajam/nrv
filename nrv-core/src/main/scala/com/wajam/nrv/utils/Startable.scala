package com.wajam.nrv.utils

/**
 * A trait which is extended by components that can be started and stopped.
 */
trait Startable {

  /**
   * Start this component.
   */
  def start()

  /**
   * Stop this component.
   */
  def stop()
}
