package com.wajam.nrv.data

/**
 * Message received from a remoter node
 */
class InMessage extends Message {
  protected[nrv] var replyCallback: (OutMessage => Unit) = null

  def reply(data: (String, Any)*) {
    this.reply(new OutMessage(data))
  }

  def reply(message: OutMessage) {
    if (replyCallback == null)
      throw new Exception("Called reply on a message with no reply callback")

    this.replyCallback(message)
  }
}
