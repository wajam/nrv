package com.wajam.nrv.service

import com.wajam.nrv.UnavailableException
import java.util.concurrent.{TimeUnit, Future}
import com.wajam.nrv.data.{MessageType, Message, OutRequest, InRequest}

/**
 * Action that binds a path to a callback. This is analogous to a RPC endpoint function,
 * but uses path to locale functions instead of functions name.
 */
class Action(var path: ActionPath, onReceive: ((InRequest) => Unit)) extends ActionSupport {
  def matches(path: ActionPath) = this.path.matchesPath(path)._1

  private def initOutRequest(request:OutRequest) {
    request.source = this.cluster.localNode
    request.serviceName = this.service.name
  }

  def call(request: OutRequest) {
    this.checkSupported()

    // initialize request
    this.initOutRequest(request)
    request.path = this.path.buildPath(request)
    request.function = MessageType.FUNCTION_CALL

    // resolve endpoints
    this.resolver.handleOutgoing(this, request)
    if (request.destination.size == 0)
      throw new UnavailableException

    // add request to router (for response)
    // TODO: shouldn't add if no response expected??
    this.cluster.router !? request

    this.protocol.handleOutgoing(this, request)
  }

  def call(data: Map[String, Any], onReceive: (InRequest => Unit) = null) {
    this.call(new OutRequest(data, onReceive))
  }

  def call(data: Map[String, Any]):Future[InRequest] = {
    new Future[InRequest] {
      def cancel(mayInterruptIfRunning: Boolean): Boolean = false

      def isCancelled: Boolean = false

      def isDone: Boolean = false

      def get(): InRequest = null

      def get(timeout: Long, unit: TimeUnit): InRequest = null
    }
  }

  def handleIncomingRequest(inRequest: InRequest, outRequest: Option[OutRequest] = None) {
    outRequest match {
      case None =>
        inRequest.replyCallback = (respRequest => {
          this.initOutRequest(respRequest)
          respRequest.path = inRequest.path
          respRequest.function = MessageType.FUNCTION_RESPONSE
          respRequest.rendezvous = inRequest.rendezvous

          // TODO: shouldn't be like that. Source may not be a member...
          respRequest.destination = new Endpoints(Seq(new ServiceMember(0, inRequest.source)))

          this.protocol.handleOutgoing(this, respRequest)
        })
        this.onReceive(inRequest)

      case Some(originalRequest) =>
        originalRequest.handleReply(inRequest)
    }
  }
}
