package com.wajam.nrv.cluster


/**
 * Cluster manager that is responsible of persisting and distributing services and nodes
 * membership across the cluster.
 */
abstract class ClusterManager {
  protected var cluster: Cluster = null

  def init(cluster: Cluster) {
    this.cluster = cluster
  }

  def start()

  def stop()

  protected def listServiceMembers = for ((_, service) <- cluster.services; member <- service.members) yield member

}
