package com.wajam.nrv.cluster

import com.wajam.nrv.service.{ServiceMember, MemberStatus, Service}

/**
 * Static cluster (fixed number of nodes, fixed address)
 */
class StaticClusterManager extends ClusterManager {

  protected def initializeMembers() {
    allMembers.foreach {
      case (service, member) => {
        member.trySetStatus(MemberStatus.Up)
        member.setStatus(MemberStatus.Up, triggerEvent = true)
      }
    }
  }

  /**
   * Add members by string.
   * @param service Service in which we want to add members
   * @param members List of members, formatted like: token:node_host:service=port,service=port;token:...
   */
  def addMembers(service: Service, members: Iterable[String]) {
    members.foreach(strMember => service.addMember(ServiceMember.fromString(strMember)))
  }
}
