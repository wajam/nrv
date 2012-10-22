package com.wajam.nrv.cluster

import com.wajam.nrv.service.{ServiceMember, MemberStatus, Service}

/**
 * Static cluster (fixed number of nodes, fixed address)
 */
class StaticClusterManager extends ClusterManager {
  protected var started = false

  override def start() {
    this.started = true
    super.start()
  }

  protected def initializeMembers() {
    for (member <- allMembers)
      member.setStatus(MemberStatus.Up, triggerEvent = false)
  }

  override def addMember(service: Service, member: ServiceMember): ServiceMember = {
    if (this.started)
      throw new Exception("Can't add member to a static cluster after started")

    super.addMember(service, member)
  }

  /**
   * Add members by string.
   * @param service Service in which we want to add members
   * @param members List of members, formatted like: token:node_host:service=port,service=port;token:...
   */
  def addMembers(service: Service, members: Iterable[String]) {
    members.foreach(strMember => addMember(service, ServiceMember.fromString(strMember)))
  }

}
