package com.wajam.nrv.data

/**
 * Message sent to a remote node
 */
class OutMessage(data: Iterable[(String, Any)] = None, onReply: (InMessage, Option[Exception]) => Unit = null) extends Message(data) {
  def handleReply(message: InMessage) {
    if (this.onReply != null)
      this.onReply(message, message.error)
  }
}
