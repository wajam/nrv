package com.wajam.nrv.data

/**
 * Message received from a remote node
 */
class InMessage(params: Iterable[(String, Any)] = null,
                meta: Iterable[(String, Any)] = null,
                data: Any = null) extends Message(params, meta, data) {

  protected[nrv] var replyCallback: (OutMessage => Unit) = null
  protected[nrv] var matchingOutMessage:Option[OutMessage] = None

  def reply(params: Iterable[(String, Any)],
            meta: Iterable[(String, Any)] = null,
            data: Any = null,
            code: Int = 200) {
    this.reply(new OutMessage(params, meta, data, code))
  }

  def reply(message: OutMessage) {
    if (replyCallback == null)
      throw new Exception("Called reply on a message with no reply callback")

    this.replyCallback(message)
  }
}
