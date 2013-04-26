package com.wajam.nrv.consistency

import com.wajam.nrv.service.{MessageHandler, Service}
import com.wajam.nrv.utils.{VotableEvent, Event}
import com.wajam.nrv.cluster.Cluster
import com.wajam.nrv.utils.timestamp.Timestamp
import com.wajam.nrv.data.Message

/**
 * Manage consistency inside the cluster based on events
 */
abstract class Consistency extends MessageHandler {

  var cluster: Cluster = null
  var bindedServices = List[Service]()

  def bindService(service: Service) {
    // keep track of cluster
    if (cluster == null) {
      cluster = service.cluster
    }

    if (!bindedServices.contains(service)) {
      bindedServices :+= service
      service.addObserver(this.serviceEvent)
    }
  }

  def serviceEvent(event: Event) {
    event match {
      case ve: VotableEvent =>
        ve.vote(pass = true)
      case _ =>
    }
  }

  def start()

  def stop()
}