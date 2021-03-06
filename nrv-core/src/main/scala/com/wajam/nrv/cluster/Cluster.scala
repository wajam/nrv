package com.wajam.nrv.cluster

import com.wajam.nrv.service._
import com.wajam.commons.Logging
import com.wajam.nrv.consistency.ConsistencyOne
import com.wajam.nrv.protocol.{NrvProtocol, Protocol}
import com.wajam.nrv.protocol.codec.HybridCodec
import com.wajam.commons.Observable
import com.wajam.tracing.Tracer

/**
 * A cluster composed of services that are provided by nodes.
 */
class Cluster(val localNode: LocalNode,
              val clusterManager: ClusterManager,
              actionSupportOptions: ActionSupportOptions = new ActionSupportOptions(),
              optDefaultProtocol: Option[Protocol] = None)
  extends ActionSupport with Logging with Observable {

  // assign default resolver, switchboard, etc.
  applySupport(cluster = Some(this), switchboard = Some(new Switchboard),
    resolver = Some(new Resolver), tracer = Some(new Tracer), consistency = Some(new ConsistencyOne),
    responseTimeout = Some(1000L), nrvCodec = Some(new HybridCodec), supportedProtocols = Some(Set()))

  applySupportOptions(actionSupportOptions)

  var services = Map[String, Service]()
  var protocols = Map[String, Protocol]()

  // initialize manager
  clusterManager.init(cluster)

  def isLocalNode(node: Node) = node == localNode

  // recover alternative protocol or get the default one
  val defaultProtocol = optDefaultProtocol.getOrElse({
    new NrvProtocol(this.localNode, 10000, 100)
  })

  // register default protocol
  this.registerProtocol(defaultProtocol, default = true)


  def registerProtocol(protocol: Protocol, default: Boolean = false) {
    this.protocols += (protocol.name -> protocol)

    if (default) {
      this.applySupport(protocol = Some(protocol))
    }
  }

  def getService(name: String): Service = this.services(name)

  def registerService(service: Service): Service = {
    service.supportedBy(this)
    service.addParentObservable(this)
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

  def stop(timeOutInMs: Long = 0L) {
    clusterManager.leave(timeOutInMs)

    for ((name, service) <- this.services) {
      service.stop()
    }

    for ((name, protocol) <- this.protocols) {
      protocol.stop()
    }

    clusterManager.stop()
  }
}
