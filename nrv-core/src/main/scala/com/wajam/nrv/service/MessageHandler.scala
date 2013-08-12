package com.wajam.nrv.service

import com.wajam.nrv.data.{InMessage, OutMessage}


/**
 * This class defines a common trait for all classes that send, receive or handle messages of various protocols
 * (nrv messages, http messages, ...). See Protocol implementations for more details on the exact mechanism used
 * to exchange messages. At this level, we assume the message has been decoded and is entirely decoupled from the
 * protocol. Once the message is decoded, it is stored into a concrete Message. There are 2 concrete Message types
 * (InMessages and OutMessages) and 2 messages content types (FUNCTION_CALL, FUNCTION_RESPONSE), for a total of 4
 * possible combinations, The following figure shows how the 4 different kind of messages are used in a typical
 * message exchange.
 *   _________                  _________
 *  |         |OUT           IN|         |
 *  |         | -----CALL----> |         |
 *  | Node #1 |                | Node #2 |
 *  |         |IN           OUT|         |
 *  |         | <--RESPONSE--- |         |
 *  ¯¯¯¯¯¯¯¯¯¯                 ¯¯¯¯¯¯¯¯¯¯
 *      - A typical message exchange -
 *
 *  Note: CALL/RESPONSE types may also be referred to as REQUEST/RESPONSE elsewhere
 *
 *  This trait uses a pattern similar to a chain of responsibility. Every message handling method may specify a
 *  next() function to be executed afterwards. This pattern lets us chain together multiple handlers, allowing
 *  different classes to apply different logic to the message in a transparent manner.
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
