package com.wajam.nrv.cluster

import com.wajam.nrv.service.{ServiceMember, MemberStatus, Service}

/**
 * Static cluster (fixed number of nodes, fixed address)
 */
class StaticClusterManager extends ClusterManager {

  protected def initializeMembers() {
    for (member <- allMembers)
      member.setStatus(MemberStatus.Up, triggerEvent = false)
  }

  // make adding members public since it's done at start by application
  override def addMember(service: Service, token: Long, node: Node): ServiceMember = {
    if (this.started)
      throw new Exception("Can't add member to a static cluster after started")

    super.addMember(service, token, node)
  }

  /**
   * Add members by string.
   * @param service Service in which we want to add members
   * @param members List of members, formatted like: token:node_host:service=port,service=port;token:...
   */
  def addMembers(service: Service, members: Iterable[String]) {
    for (memberString <- members) {
      val Array(strToken, strHost, strPorts) = memberString.split(":")

      var mapPorts = Map[String, Int]()
      for (strSrvPort <- strPorts.split(",")) {
        val Array(strService, strPort) = strSrvPort.split("=")
        mapPorts += (strService -> strPort.toInt)
      }

      this.addMember(service, strToken.toLong, new Node(strHost, mapPorts))
    }
  }

}
