package com.wajam.nrv.cluster

import com.wajam.nrv.service.{MemberStatus, ServiceMember, Service}
import scala.annotation.tailrec
import com.wajam.nrv.Logging

/**
 * Cluster manager that is responsible of persisting and distributing services and nodes
 * membership across the cluster.
 */
abstract class ClusterManager extends Logging {
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

  @tailrec
  private def waitForStatusDown(startTime: Long = System.currentTimeMillis()) {
    if ((System.currentTimeMillis() - startTime) > ClusterManager.timeoutInMs) {
      throw new RuntimeException("Timeout waiting for all service members to switch to down status.")
    }
    if(this.allMembers.map(_._2).count(member => cluster.isLocalNode(member.node) && member.status != MemberStatus.Down) > 0) {
      Thread.sleep(ClusterManager.sleepTimeInMs)
      waitForStatusDown(startTime)
    }
  }

  protected def allServices = cluster.services.values

  protected def allMembers: Iterable[(Service, ServiceMember)] =
    cluster.services.values.flatMap(service =>
      service.members.map((service, _))
    )

  def trySetServiceMemberStatusDown(service: Service, member: ServiceMember)

  def shutdownCluster() {
    shutDownMembers()
    try {
      waitForStatusDown()
    } catch {
      case e: Exception => warn("The clean shutdown has timed out! Some nodes weren't able to be set to Down.", e)
    }

  }

  protected def shutDownMembers() {}
}

private object ClusterManager {
  val sleepTimeInMs: Long = 100
  val timeoutInMs: Long = 5000
}