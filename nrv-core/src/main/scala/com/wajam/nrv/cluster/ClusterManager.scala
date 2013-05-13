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
        this.initializeWrapper()
        started = true
        true
      } else false
    }
  }

  private def initializeWrapper(){
    this.initializeMembers()
    cluster.services.values.foreach(_.addObserver(DisabledNodeAlerter.handleEvent))
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
}

/**
 * Manages a counter metric that keeps track of the status of all ServiceMembers, and counts the ones
 * changing to "Down" status.
 */
object DisabledNodeAlerter extends Instrumented {
  private val statusDownCounter = metrics.counter("ServiceMember.Status.Down")

  def handleEvent(event: Event) {
    event match {
      case StatusTransitionEvent(_,from: MemberStatus,to: MemberStatus) => {
        if(to == MemberStatus.Down) {
          statusDownCounter += 1
        } else if(from == MemberStatus.Down){
          statusDownCounter -= 1
        }
      }
      case _ =>
    }
  }
}

