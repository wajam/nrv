package com.wajam.nrv.cluster

import com.wajam.nrv.service._
import com.wajam.nrv.service.{Action, Service}
import com.wajam.nrv.data.InMessage
import com.wajam.nrv.Logging
import com.wajam.nrv.protocol.{ListenerException, ProtocolMessageListener, NrvProtocol, Protocol}

/**
 * A cluster composed of services that are provided by nodes.
 */
class Cluster(var localNode: Node,
              var clusterManager: ClusterManager,
              switchboard: Switchboard = new Switchboard,
              resolver: Resolver = new Resolver)
  extends ActionSupport with ProtocolMessageListener with Logging {
  //TODO HERE
  applySupport(cluster = Some(this), resolver = Some(resolver), switchboard = Some(switchboard))

  var services = Map[String, Service]()
  var protocols = Map[String, Protocol]()

  // register default protocol, which is nrv
  this.registerProtocol(new NrvProtocol(this.localNode, this), true)

  def registerProtocol(protocol: Protocol, default: Boolean = false) {
    this.protocols += (protocol.name -> protocol)

    if (default) {
      this.applySupport(protocol = Some(protocol))
    }
  }

  def messageReceived(inMessage: InMessage) {
    val action = cluster.getAction(inMessage.actionURL, inMessage.method)
    if (action != null) {
      trace("Received an incoming message for path {} with method {}.", inMessage.actionURL.toString, inMessage.method.toString)
      action.callIncomingHandlers(inMessage)
    } else {
      warn("Received an incoming for path {}, but couldn't find action", inMessage.actionURL.toString)
      throw new ListenerException("Route not found.")
    }
  }

  def getService(name: String): Service = this.services(name)

  def registerService(service: Service): Service = {
    service.supportedBy(this)
    this.services += (service.name -> service)
    service
  }

  def getAction(url: ActionURL, method: String): Action = {
    val service = services.get(url.service)
    if (service == None)
      return null

    val action = service.get.findAction(url.path, method)

    action.getOrElse(null)
  }

  def start() {
    clusterManager.start()

    for ((name, protocol) <- this.protocols) {
      protocol.start()
    }

    for ((name, service) <- this.services) {
      service.start()
    }
  }

  def stop() {
    for ((name, protocol) <- this.protocols) {
      protocol.stop()
    }
  }
}
