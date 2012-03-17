package com.appaquet.nrv.service

import com.appaquet.nrv.data.Message


/**
 * Incoming and outgoing messages handler.
 */
trait MessageHandler {
  def handleIncoming(action: Action, message: Message)

  def handleOutgoing(action: Action, message: Message)
}
