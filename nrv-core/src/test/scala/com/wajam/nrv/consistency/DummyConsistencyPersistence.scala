package com.wajam.nrv.consistency

import com.wajam.nrv.cluster.Node
import com.wajam.nrv.service.ServiceMember

class DummyConsistencyPersistence(serviceCache: ServiceMemberClusterStorage,
                                  currentExplicitReplicasMapping: => Map[Long, List[Node]] = Map()) extends ConsistencyPersistence {

  private var lags: Map[(Long, Node), Int] = Map()

  def start(): Unit = {}

  def stop(): Unit = {}

  def explicitReplicasMapping: Map[Long, List[Node]] = currentExplicitReplicasMapping

  /**
   * Returns the number of seconds the specified replica lag behind the master service member.
   */
  def replicationLagSeconds(token: Long, node: Node): Option[Int] = {
    lags.get((token, node))
  }

  /**
   * Update the number of seconds the specified replica lag behind the master service member. This change is visible to
   * all nodes in the cluster.
   */
  def updateReplicationLagSeconds(token: Long, node: Node, lag: Int): Unit = {
    synchronized(lags += (token, node) -> lag)
  }

  /**
   * Change the master service member. This change is visible to all nodes in the cluster.
   */
  def changeMasterServiceMember(token: Long, node: Node): Unit = {
    serviceCache.addMember(new ServiceMember(token, node))
  }
}
