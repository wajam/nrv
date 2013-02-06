package com.wajam.nrv.data

/**
 * Message sent to a remote node
 */
class OutMessage(params: Iterable[(String, Seq[String])] = null,
                 meta: Iterable[(String, Seq[String])] = null,
                 data: Any = null,
                 code: Int = 200,
                 onReply: (InMessage, Option[Exception]) => Unit = null,
                 val responseTimeout: Long = 1000) extends Message(params, meta, data, code) {

  var sentTime:Long = 0

  protected[nrv] def handleReply(message: InMessage) {
    if (this.onReply != null)
      this.onReply(message, message.error)
  }
}
