package com.wajam.nrv.service

import com.wajam.nrv.cluster.Node
import java.io.Serializable

/**
 * Node that is member of a service, at a specific position (token) in
 * the consistent hashing ring of the service.
 */
class ServiceMember(val token: Long, val node: Node, var status: MemberStatus = MemberStatus.DOWN) extends Serializable {
}

class MemberStatus extends Serializable {
}

object MemberStatus {
  val DOWN = MemberDown
  val JOINING = MemberJoining
  val UP = MemberUp
  val LEAVING = MemberLeaving
}

object MemberDown extends MemberStatus

object MemberJoining extends MemberStatus

object MemberUp extends MemberStatus

object MemberLeaving extends MemberStatus


