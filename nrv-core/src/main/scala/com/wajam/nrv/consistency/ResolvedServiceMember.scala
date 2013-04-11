package com.wajam.nrv.consistency

import com.wajam.nrv.service.{ServiceMember, Service, TokenRange}

case class ResolvedServiceMember(serviceName: String, token: Long, ranges: Seq[TokenRange]) {
  lazy val scopeName = serviceName.replace(".", "-") + "." + token
}

object ResolvedServiceMember {
  def apply(service: Service, member: ServiceMember): ResolvedServiceMember = {
    val name = service.name
    val token = member.token
    val ranges = service.getMemberTokenRanges(service.getMemberAtToken(token).get)
    ResolvedServiceMember(name, token, ranges)
  }
}