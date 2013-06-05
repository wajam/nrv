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

  //deselects all the replica in the selectedReplica list (sets selected = false) expect for the first one
  //which is considered to be the master
  protected[nrv] def deselectAllReplicasButFirst() {
    replicas.slice(1, replicas.size).foreach(_.selected = false)
  }

  protected[nrv] def deselectAllReplicasButOne() {
    selectedReplicas.slice(1, replicas.size).foreach(_.selected = false)
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
