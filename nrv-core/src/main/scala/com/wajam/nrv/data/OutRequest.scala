package com.wajam.nrv.data

/**
 * Request sent to a remote node
 */
class OutRequest(data: Iterable[(String, Any)] = None, onReceive: (InRequest => Unit) = null) extends Message(data) {
}
