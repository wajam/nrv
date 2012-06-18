package com.wajam.nrv.data

/**
 * Message sent to a remote node
 */
class OutMessage(params: Iterable[(String, Any)] = null,
                 meta: Iterable[(String, Any)] = null,
                 data: Any = null,
                 code: Int = 200,
                 onReply: (InMessage, Option[Exception]) => Unit = null) extends Message(params, meta, data) {

  var timeoutTime:Long = 1000
  var sentTime:Long = 0

  protected[nrv] def handleReply(message: InMessage) {
    if (this.onReply != null)
      this.onReply(message, message.error)
  }
}
