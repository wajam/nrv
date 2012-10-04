package com.wajam.nrv.service

import com.wajam.nrv.cluster.Node
import java.io.Serializable

/**
 * Node that is member of a service, at a specific position (token) in
 * the consistent hashing ring of the service.
 */
class ServiceMember(val token: Long, val node: Node, var status: MemberStatus = MemberStatus.Down) extends Serializable {
}

sealed trait MemberStatus extends Serializable;

object MemberStatus {
  case object Down extends MemberStatus

  case object Joining extends MemberStatus

  case object Up extends MemberStatus

  case object Leaving extends MemberStatus
}


