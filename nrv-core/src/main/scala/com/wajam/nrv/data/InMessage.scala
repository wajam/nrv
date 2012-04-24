package com.wajam.nrv.data

/**
 * Message received from a remote node
 */
class InMessage extends Message {
  protected[nrv] var replyCallback: (OutMessage => Unit) = null
  protected[nrv] var matchingOutMessage:Option[OutMessage] = None

  def reply(data: (String, Any)*) {
    this.reply(new OutMessage(data))
  }

  def reply(message: OutMessage) {
    if (replyCallback == null)
      throw new Exception("Called reply on a message with no reply callback")

    this.replyCallback(message)
  }
}
