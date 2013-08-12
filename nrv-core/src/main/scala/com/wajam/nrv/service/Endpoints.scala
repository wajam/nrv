package com.wajam.nrv.service

import com.wajam.nrv.cluster.Node


/**
 * Endpoints to which a message are sent, composed of a shard (position in the ring, associated
 * to a token) and then for shard, there are replicas (actual node, composed of an ip and
 * nrv port).
 */
class Endpoints(val shards: Seq[Shard] = Seq()) extends Serializable {

  def selectedReplicas: Seq[Replica] = shards.map(_.selectedEndpoints).flatten

  def replicas: Seq[Replica] = shards.map(_.replicas).flatten

  /**
   * Deselects all slave destinations. The first one (the master) remains unchanged.
   */
  protected[nrv] def deselectAllReplicasButFirst() {
    replicas.drop(1).foreach(_.selected = false)
  }

  /**
   * Deselects all possible destinations, except the first one that's currently available (master or slave).
   */
  protected[nrv] def deselectAllReplicasButOne() {
    selectedReplicas.drop(1).foreach(_.selected = false)
  }

  /**
   * Deselects everything. no exceptions.
   */
  protected[nrv] def deselectAllReplicas() {
    replicas.foreach(_.selected = false)
  }
}

object Endpoints {
  val EMPTY = new Endpoints()
}

/**
 * A shard is associated to multiple replicas, since it has slaves for redundancy.
 */
class Shard(val token: Long, val replicas: Seq[Replica]) extends Serializable {
  def selectedEndpoints: Seq[Replica] = replicas.filter(_.selected)
}

class Replica(val token: Long, val node: Node, var selected: Boolean = true) extends Serializable
