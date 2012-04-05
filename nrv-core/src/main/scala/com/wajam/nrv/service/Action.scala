package com.wajam.nrv.service

import com.wajam.nrv.data.{OutRequest, InRequest}
import com.wajam.nrv.UnavailableException

/**
 * Action that binds a path to a callback. This is analogous to a RPC endpoint function,
 * but uses path to locale functions instead of functions name.
 */
class Action(var path: ActionPath, onReceive: ((InRequest) => Unit)) extends ActionSupport {
  def matches(path: ActionPath) = this.path.matchesPath(path)._1

  def call(request: OutRequest) {
    this.checkSupported()

    // resolve endpoints
    request.path = this.path.buildPath(request)
    this.resolver.handleOutgoing(this, request)
    if (request.destination.size == 0)
      throw new UnavailableException

    // add request to router (for response)
    // TODO: shouldn't add if no response expected??
    this.cluster.router !? request

    this.protocol.handleOutgoing(this, request)
  }

  def call(data: (String, Any)*)(onReceive: (InRequest => Unit) = null) {
    this.call(new OutRequest(data, onReceive))
  }

  def handleIncomingRequest(inRequest: InRequest, outRequest: Option[OutRequest] = None) {
    if (outRequest != None)
      outRequest.get.handleReply(inRequest)
    else
      this.onReceive(inRequest)
  }
}
