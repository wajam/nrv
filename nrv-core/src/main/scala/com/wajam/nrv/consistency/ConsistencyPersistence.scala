package com.wajam.nrv.consistency

import com.wajam.nrv.cluster.Node

/**
 * Trait to read and update some cluster wide configuration and state necessary for consistent services.
 */
trait ConsistencyPersistence {

  def start()

  def stop()

  /**
   * Returns the list of all replicas (master service member + slaves replicas) per shard token.
   */
  def explicitReplicasMapping: Map[Long, List[Node]]

  /**
   * Returns the number of seconds the specified replica lag behind the master service member.
   */
  def replicationLagSeconds(token: Long, node: Node): Option[Int]

  /**
   * Update the number of seconds the specified replica lag behind the master service member. This change is visible to
   * all nodes in the cluster.
   */
  def updateReplicationLagSeconds(token: Long, node: Node, lag: Int)

  /**
   * Change the master service member. This change is visible to all nodes in the cluster.
   */
  def changeMasterServiceMember(token: Long, node: Node)
}

object ConsistencyPersistence {

  val Noop = new ConsistencyPersistence {

    def start() = Unit

    def stop() = Unit

    def explicitReplicasMapping = Map()

    def replicationLagSeconds(token: Long, node: Node) = None

    def updateReplicationLagSeconds(token: Long, node: Node, lag: Int) = Unit

    def changeMasterServiceMember(token: Long, node: Node) = Unit
  }
}
