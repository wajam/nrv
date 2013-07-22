package com.wajam.nrv.service

import com.wajam.nrv.cluster.Node


/**
 * Endpoints to which a message are sent, composed of shards (position in the ring, associated
 * to a token) and then for shard, there are replicas (actual node, composed of an ip and
 * nrv port).
 */
class Endpoints(val shards: Seq[Shard] = Seq()) extends Serializable {

  def selectedReplicas: Seq[Replica] = shards.map(_.selectedEndpoints).flatten

  def replicas: Seq[Replica] = shards.map(_.replicas).flatten

  /**
   * Deselects all the replica in the replicas list (sets selected = false) except for the first one which is
   * considered to be the only routing possibility. This method act on all replicas not just the selected
   * ones. If the first replica is unselected, all replicas will be deselected after calling this method even if a
   * subsequent replica was previously selected.
   */
  protected[nrv] def deselectAllReplicasButFirst() {
    replicas.drop(1).foreach(_.selected = false)
  }

  /**
   * Deselects all the replicas (sets selected = false) except for the first one in selectedReplicas list. If more than
   * one replica was selected before calling this method, only the first originally selected remain selected after.
   */
  protected[nrv] def deselectAllReplicasButOne() {
    selectedReplicas.drop(1).foreach(_.selected = false)
  }

  protected[nrv] def deselectAllReplicas() {
    replicas.foreach(_.selected = false)
  }

}

object Endpoints {
  val EMPTY = new Endpoints()
}

class Shard(val token: Long, val replicas: Seq[Replica]) extends Serializable {
  def selectedEndpoints: Seq[Replica] = replicas.filter(_.selected)
}

class Replica(val token: Long, val node: Node, var selected: Boolean = true) extends Serializable
