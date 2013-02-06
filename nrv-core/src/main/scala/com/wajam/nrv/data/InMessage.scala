package com.wajam.nrv.data

/**
 * Message received from a remote node
 */
class InMessage(params: Iterable[(String, Seq[String])] = null,
                meta: Iterable[(String, Seq[String])] = null,
                data: Any = null) extends Message(params, meta, data) {

  protected[nrv] var replyCallback: (OutMessage => Unit) = null
  protected[nrv] var matchingOutMessage: Option[OutMessage] = None

  def reply(params: Iterable[(String, Seq[String])],
            meta: Iterable[(String, Seq[String])] = null,
            data: Any = null,
            code: Int = 200) {
    this.reply(new OutMessage(params, meta, data, code))
  }

  def replyWithError(error: Exception,
                     params: Iterable[(String, Seq[String])] = null,
                     meta: Iterable[(String, Seq[String])] = null,
                     data: Any = null,
                     code: Int = 500) {
    val response = new OutMessage(params, meta, data, code)
    response.error = Some(error)
    this.reply(response)
  }

  def reply(message: OutMessage) {
    if (replyCallback == null)
      throw new Exception("Called reply on a message with no reply callback")

    this.replyCallback(message)
  }
}
