package com.wajam.nrv.consistency

import com.wajam.nrv.service.{ServiceMember, Service, TokenRange}

case class ResolvedServiceMember(serviceName: String, token: Long, ranges: Seq[TokenRange]) {
  lazy val scopeName = serviceName.replace(".", "-") + "." + token
}

object ResolvedServiceMember {
  def apply(service: Service, token: Long): ResolvedServiceMember = {
    val name = service.name
    val ranges = service.getMemberTokenRanges(service.getMemberAtToken(token).get)
    ResolvedServiceMember(name, token, ranges)
  }
  def apply(service: Service, member: ServiceMember): ResolvedServiceMember = {
    ResolvedServiceMember(service, member.token)
  }
}