package com.wajam.nrv.consistency

import com.wajam.nrv.service.TokenRange

case class ResolvedServiceMember(serviceName: String, token: Long, ranges: Seq[TokenRange]) {
  lazy val scopeName = serviceName + "." + token
}
