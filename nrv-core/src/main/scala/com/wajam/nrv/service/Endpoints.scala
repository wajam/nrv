package com.wajam.nrv.service

/**
 * Endpoints that handle an action. Some action are managed by a single node,
 * but all backup nodes are considered endpoints too in order to handle the
 * high availability. Consensus manager handles replication to these endpoints.
 */
class Endpoints(members: Seq[ServiceMember]) extends Serializable {
  def size = members.size

  def apply(pos: Int) = members(pos)
}

object Endpoints {
  val empty = new Endpoints(List())
}
