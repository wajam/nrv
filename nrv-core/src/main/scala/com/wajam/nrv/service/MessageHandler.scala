package com.wajam.nrv.service

import com.wajam.nrv.data.Message


/**
 * Incoming and outgoing messages handler.
 */
trait MessageHandler {
  def handleIncoming(action: Action, message: Message)

  def handleOutgoing(action: Action, message: Message)
}
