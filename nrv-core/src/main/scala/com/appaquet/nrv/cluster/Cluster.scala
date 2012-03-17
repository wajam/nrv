package com.appaquet.nrv.cluster

import collection.mutable.HashMap
import com.appaquet.nrv.service._
import com.appaquet.nrv.protocol.{NrvProtocol, Protocol}

/**
 * A cluster composed of services that are provided by nodes.
 */
class Cluster(var localNode: Node, resolver: Option[Resolver] = None) extends ActionSupport {
  applySupport(cluster = Some(this), resolver = resolver)

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
