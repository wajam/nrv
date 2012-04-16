package com.wajam.nrv.data

/**
 * Request sent to a remote node
 */
class OutRequest(data: Iterable[(String, Any)] = None, onReply: (InRequest, Option[Exception]) => Unit = null) extends Message(data) {

  def handleReply(request:InRequest) {
    if (this.onReply != null)
      this.onReply(request, request.error)
  }
}
