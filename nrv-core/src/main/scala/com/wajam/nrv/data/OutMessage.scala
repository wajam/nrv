package com.wajam.nrv.data

/**
 * Message sent to a remote node
 */
class OutMessage(data: Iterable[(String, Any)] = None, onReply: (InMessage, Option[Exception]) => Unit = null) extends Message(data) {
  var timeoutTime:Long = 1000
  var sentTime:Long = 0

  protected[nrv] def handleReply(message: InMessage) {
    if (this.onReply != null)
      this.onReply(message, message.error)
  }
}
