package com.wajam.nrv.data

/**
 * Message received from a remote node
 */
class InMessage(params: Iterable[(String, MValue)] = null,
                meta: Iterable[(String, MValue)] = null,
                data: Any = null) extends Message(params, meta, data) {

  protected[nrv] var replyCallback: (OutMessage => Unit) = null
  protected[nrv] var matchingOutMessage: Option[OutMessage] = None

  def reply(params: Iterable[(String, MValue)],
            meta: Iterable[(String, MValue)] = null,
            data: Any = null,
            code: Int = 200) {
    this.reply(new OutMessage(params, meta, data, code))
  }

  def replyWithError(error: Exception,
                     params: Iterable[(String, MValue)] = null,
                     meta: Iterable[(String, MValue)] = null,
                     data: Any = null,
                     code: Int = 500) {
    val response = new OutMessage(params, meta, data, code)
    response.error = Some(error)
    this.reply(response)
  }

  def reply(message: OutMessage) {
    if (replyCallback == null)
      throw new Exception("Called reply on a message with no reply callback")
    //set protocolName to the incoming protocol name to reuse the same protocol to send the response
    message.protocolName = protocolName
    this.replyCallback(message)
  }
}
