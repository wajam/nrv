package com.wajam.nrv.service

import com.wajam.nrv.cluster.Node


/**
 * Endpoints to which a message are sent, composed of a shard (position in the ring, associated
 * to a token) and then for shard, there are replicas (actual node, composed of an ip and
 * nrv port).
 */
case class Endpoints(shards: Seq[Shard] = Seq()) extends Serializable {

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

  /**
   * Deselects all destinations except the one provided as argument.
   */
  protected[nrv] def deselectAllReplicasBut(remainder: Replica) {
    replicas.filterNot(_ == remainder).foreach(_.selected = false)
  }
}

object Endpoints {
  val EMPTY = new Endpoints()
}

/**
 * A shard is associated to multiple replicas, since it has slaves for redundancy.
 */
case class Shard(token: Long, replicas: Seq[Replica]) extends Serializable {
  def selectedEndpoints: Seq[Replica] = replicas.filter(_.selected)
}

case class Replica(token: Long, node: Node, var selected: Boolean = true) extends Serializable
