package com.wajam.nrv.cluster

import actors.Actor
import collection.mutable.Map
import com.wajam.nrv.data.{InRequest, OutRequest}
import com.wajam.nrv.Logging

/**
 * Handle incoming requests to different actions
 */
class Router(cluster: Cluster) extends Actor with Logging {
  private var requests = Map[Int, OutRequest]()
  private var id = 0

  // TODO: timeouts (w/cleanup)

  def act() {
    while (true) {
      receive {
        case outRequest: OutRequest =>
          this.id += 1
          outRequest.rendezvous = this.id
          this.requests += (this.id -> outRequest)

          if (this.id > Int.MaxValue)
            this.id = 0

          sender ! true

        case inRequest: InRequest =>
          // check for rendez-vous
          var optReq:Option[OutRequest] = None
          if (inRequest.rendezvous > 0) {
            optReq = this.requests.remove(inRequest.rendezvous)
            if (optReq == None) {
                warn("Received a incoming request with a rendez-vous, but with no matching outgoing request: {}", inRequest)
            }
          }

          val action = cluster.getAction(inRequest.actionURL)
          if (action != null) {
            action.handleIncomingRequest(inRequest, optReq)
          } else {
            warn("Received a incoming for path {}, but couldn't find action", inRequest.actionURL)
          }

          sender ! true
      }
    }
  }
}
