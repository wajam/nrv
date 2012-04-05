package com.wajam.nrv.cluster

import com.wajam.nrv.service._
import com.wajam.nrv.protocol.{NrvProtocol, Protocol}
import com.wajam.nrv.service.{Action, Service}
import com.wajam.nrv.data.Message

/**
 * A cluster composed of services that are provided by nodes.
 */
class Cluster(var localNode: Node, var clusterManager: ClusterManager) extends ActionSupport {
  applySupport(cluster = Some(this), resolver = Some(new Resolver))

  var router = new Router(this)
  var services = Map[String, Service]()
  var protocols = Map[String, Protocol]()

  // register default protocol, which is nrv
  this.registerProtocol(new NrvProtocol(this), true)

  def registerProtocol(protocol: Protocol, default: Boolean = false) {
    this.protocols += (protocol.name -> protocol)

    if (default) {
      this.applySupport(protocol = Some(protocol))
    }
  }

  def route(message: Message) {
    this.router ! message
  }

  def getService(name: String): Service = this.services(name)

  def addService(service: Service): Service = {
    service.supportedBy(this)
    this.services += (service.name -> service)
    service
  }

  def getAction(url: ActionURL): Action = {
    val service = services.get(url.service)
    if (service == None)
      return null

    val action = service.get.findAction(url.path)

    action.getOrElse(null)
  }

  def start() {
    this.router.start()
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
