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

  protected def allMembers = for ((_, service) <- cluster.services; member <- service.members) yield member


  /*
   * Members management
   */
  protected def addMember(service: Service, member: ServiceMember) = service.addMember(member)

}
