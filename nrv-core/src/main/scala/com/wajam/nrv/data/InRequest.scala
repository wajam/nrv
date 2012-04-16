package com.wajam.nrv.data

/**
 * Received request
 */
class InRequest extends Message {
  var replyCallback:(OutRequest => Unit) = null
  var transportReplyCallback:(OutRequest => Unit) = null

  def reply(data:(String,Any)*) {
    this.reply(new OutRequest(data))
  }

  def reply(request:OutRequest) {
    if (replyCallback == null)
      throw new Exception("Called reply on a request with no reply callback")

    this.replyCallback(request)
  }
}
