package com.wajam.nrv.cluster

import com.wajam.nrv.service.{StatusTransitionEvent, MemberStatus, ServiceMember, Service}
import com.yammer.metrics.scala.Instrumented
import com.wajam.nrv.utils.Event

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

  def start(): Boolean = {
    synchronized {
      if (!started) {
        this.initializeMembers()
        started = true
        true
      } else false
    }
  }

  protected def initializeMembers()

  def stop(): Boolean = {
    synchronized {
      if (started) {
        started = false
        true
      } else false
    }
  }

  protected def allServices = cluster.services.values

  protected def allMembers: Iterable[(Service, ServiceMember)] =
    cluster.services.values.flatMap(service =>
      service.members.map((service, _))
    )

  def trySetServiceMemberStatusDown(service: Service, member: ServiceMember)