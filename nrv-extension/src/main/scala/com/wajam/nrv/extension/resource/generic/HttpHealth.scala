package com.wajam.nrv.extension.resource.generic

import com.wajam.nrv.extension.resource.{ Index, Resource }
import com.wajam.nrv.cluster.Cluster
import com.wajam.nrv.service.MemberStatus

abstract class HttpHealth(cluster: Cluster) extends Resource("health", "", None) with Index {

  protected def contentType: String

  def index = { request =>

    def localMembers = cluster.services.values.flatMap(_.members).filter(member => cluster.isLocalNode(member.node))

    val (status, code) = if (localMembers.forall(_.status == MemberStatus.Up)) {
      ("OK", 200)
    } else {
      // e.g. "up -> 3, joining -> 1"
      val summary = localMembers.groupBy(_.status).map(e => (e._1, e._2.size)).mkString(", ")
      (summary, 503)
    }

    val headers = Map("Content-Type" -> contentType)
    val data = Map("status" -> status)
    request.reply(Map(), headers, data, code = code)
  }

}
