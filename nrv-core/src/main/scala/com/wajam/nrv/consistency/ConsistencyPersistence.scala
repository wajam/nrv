package com.wajam.nrv.consistency

import com.wajam.nrv.cluster.Node
import com.wajam.nrv.service.Service

trait ConsistencyPersistence {

  def start()

  def stop()

  /**
   * Returns the list of all replicas (master service member + slaves replicas) per shard token.
   */
  def explicitReplicasMapping: Map[Long, List[Node]] // TODO: Need caching + watch changes

  /**
   * Returns the number of seconds the specified replica lag behind the master service member.
   */
  def replicationLagSeconds(token: Long, node: Node): Option[Int]

  /**
   * Update the number of seconds the specified replica lag behind the master service member. This change is visible to
   * all nodes in the cluster.
   */
  def replicationLagSeconds_= (token: Long, node: Node, lag: Option[Int])

  /**
   * Change the master service member. This change is visible to all nodes in the cluster.
   */
  def changeMasterServiceMember(token: Long, node: Node)
}
