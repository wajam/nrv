package com.wajam.nrv.cluster

import com.wajam.nrv.service._
import com.wajam.nrv.service.{Action, Service}
import com.wajam.nrv.data.InMessage
import com.wajam.nrv.Logging
import com.wajam.nrv.protocol.{ProtocolMessageListener, NrvProtocol, Protocol}

/**
 * A cluster composed of services that are provided by nodes.
 */
class Cluster(var localNode: Node, var clusterManager: ClusterManager) extends ActionSupport
with ProtocolMessageListener with Logging {
  applySupport(cluster = Some(this), resolver = Some(new Resolver), switchboard = Some(new Switchboard))

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
      action.callIncomingHandlers(inMessage)
    } else {
      warn("Received a incoming for path {}, but couldn't find action", inMessage.actionURL.toString)
    }
  }

  def getService(name: String): Service = this.services(name)

  def addService(service: Service): Service = {
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
