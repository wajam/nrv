package com.appaquet.nrv.service

import com.appaquet.nrv.data.{OutRequest, InRequest}


/**
 * Action that binds a path to a callback. This is analogous to a RPC endpoint function,
 * but uses path to locale functions instead of functions name.
 */
class Action(onReceive: ((InRequest) => Unit)) extends ActionSupport {
  var path: String = ""

  def matches(path: String) = path == this.path

  def call(request: OutRequest) {
    this.checkSupported()

    this.resolver.handleOutgoing(this, request)
    this.protocol.handleOutgoing(this, request)
  }

  def call(data: Iterable[(String, Any)], onReceive: (InRequest => Unit) = null) {
    this.call(new OutRequest(data, onReceive))
  }

  def handleIncomingRequest(request: InRequest) {
    this.onReceive(request)
  }
}
