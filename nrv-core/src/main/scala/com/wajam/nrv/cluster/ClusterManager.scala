package com.wajam.nrv.cluster

import com.wajam.nrv.service.{MemberStatus, ServiceMember, Service}


/**
 * Cluster manager that is responsible of persisting and distributing services and nodes
 * membership across the cluster.
 */
abstract class ClusterManager {
  protected var started = false

  protected var cluster: Cluster = null

  def init(cluster: Cluster) {
    this.cluster = cluster
  }

  def start() {
    this.initializeMembers()
    this.started = true
  }

  protected def initializeMembers()

  protected def addMember(service: Service, token: Long, node: Node) = service.addMember(token, node)

  protected def allMembers = for ((_, service) <- cluster.services; member <- service.members) yield member

  protected def setMemberStatus(member: ServiceMember, newStatus: MemberStatus) {
    member.trySetStatus(newStatus, true)
  }

  def stop() {
  }

}
