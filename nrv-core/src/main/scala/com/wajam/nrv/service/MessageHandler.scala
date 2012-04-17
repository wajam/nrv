package com.wajam.nrv.service

import com.wajam.nrv.data.Message

/**
 * This class...
 *
 * User: felix
 * Date: 12/04/12
 */

trait MessageHandler {

  def handleIncoming(action: Action, message: Message) {
  }

  def handleIncoming(action: Action, message: Message, next: Unit => Unit) {
    this.handleIncoming(action, message)
    next()
  }

  def handleOutgoing(action: Action, message: Message) {
  }

  def handleOutgoing(action: Action, message: Message, next: Unit => Unit) {
    this.handleOutgoing(action, message)
    next()
  }

}
