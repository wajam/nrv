package com.wajam.nrv.cluster

import collection.mutable.HashMap
import com.wajam.nrv.service._
import com.wajam.nrv.protocol.{NrvProtocol, Protocol}
import com.wajam.nrv.service.{Action, ActionUrl, Service}

/**
 * A cluster composed of services that are provided by nodes.
 */
class Cluster(var localNode: Node, var clusterManager: ClusterManager) extends ActionSupport {
  applySupport(cluster = Some(this))

  var services = HashMap[String, Service]()
  var protocols = HashMap[String, Protocol]()

  // register default protocol, which is nrv
  this.registerProtocol(new NrvProtocol(this), true)

  def registerProtocol(protocol: Protocol, default: Boolean = false) {
    this.protocols += (protocol.name -> protocol)

    if (default) {
      this.applySupport(protocol = Some(protocol))
    }
  }

  def getService(name: String): Service = this.services(name)

  def addService(service: Service): Service = {
    service.supportedBy(this)
    this.services += (service.name -> service)
    service
  }

  def getAction(url: ActionUrl): Action = {
    val service = services.get(url.service)
    if (service == None)
      return null

    val action = service.get.findAction(url.path)

    action.getOrElse(null)
  }

  def start() {
    for ((name, protocol) <- this.protocols) {
      protocol.start()
    }
  }

  def stop() {
    for ((name, protocol) <- this.protocols) {
      protocol.stop()
    }
  }
}
