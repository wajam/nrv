package com.wajam.nrv.service

import com.wajam.nrv.data.{InMessage, OutMessage}


/**
 * This class...
 *
 * User: felix
 * Date: 12/04/12
 */

trait MessageHandler {

  def handleIncoming(action: Action, message: InMessage) {
  }

  def handleIncoming(action: Action, message: InMessage, next: Unit => Unit) {
    this.handleIncoming(action, message)
    next()
  }

  def handleOutgoing(action: Action, message: OutMessage) {
  }

  def handleOutgoing(action: Action, message: OutMessage, next: Unit => Unit) {
    this.handleOutgoing(action, message)
    next()
  }

}
