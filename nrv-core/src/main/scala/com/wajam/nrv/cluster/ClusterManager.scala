package com.wajam.nrv.cluster

import com.wajam.nrv.service.Service

/**
 * Cluster manager that is responsible of persisting and distributing services and nodes
 * membership across the cluster.
 */
abstract class ClusterManager {

  def start()

  def addMember(service: Service, token: Long, node: Node) = service.addMember(token, node)

}
