package com.wajam.nrv.cluster

import com.wajam.nrv.service.{MemberStatus, Service}


/**
 * Static cluster (fixed number of nodes, fixed address)
 */
class StaticClusterManager extends ClusterManager {

  def start() {
    // in static cluster, all nodes are up
    for (member <- this.listServiceMembers) {
      member.status = MemberStatus.Up
    }
  }

  def stop() {}

  def addMember(service: Service, token: Long, node: Node) = service.addMember(token, node)

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
