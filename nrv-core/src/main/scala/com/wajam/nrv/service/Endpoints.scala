package com.wajam.nrv.service

import com.wajam.nrv.cluster.Node


/**
 * Endpoints to which a message are sent, composed of shards (position in the ring, associated
 * to a token) and then for shard, there are replicas (actual node, composed of an ip and
 * nrv port).
 */
class Endpoints(val shards: Seq[Shard] = Seq()) extends Serializable {

  def onlineReplicas: Seq[Replica] = shards.map(_.selectedEndpoints).flatten

}

object Endpoints {
  val EMPTY = new Endpoints()
}

class Shard(val token: Long, val replicas: Seq[Replica]) extends Serializable {
  def selectedEndpoints: Seq[Replica] = replicas.filter(_.selected)
}

class Replica(val token: Long, val node: Node, var selected: Boolean = true) extends Serializable
