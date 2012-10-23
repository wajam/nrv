package com.wajam.nrv.cluster

import com.wajam.nrv.service.{ServiceMember, Service}

/**
 * Cluster manager that is responsible of persisting and distributing services and nodes
 * membership across the cluster.
 */
abstract class ClusterManager {
  protected var cluster: Cluster = null

  def init(cluster: Cluster) {
    this.cluster = cluster
  }

  def start() {
    this.initializeMembers()
  }

  protected def initializeMembers()

  def stop() {
  }

  protected def allServices = cluster.services.values

  protected def allMembers: Iterable[(Service, ServiceMember)] =
    cluster.services.values.flatMap(service =>
      service.members.map((service, _))
    )

}
