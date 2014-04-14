package com.wajam.nrv.consistency

import com.wajam.nrv.cluster.{Node, ServiceMemberVote, DynamicClusterManager}
import com.wajam.nrv.service.{MemberStatus, ServiceMember, Service}
import scala.concurrent.{ExecutionContext, Future}
import com.wajam.commons.{Logging, Event, Observable}
import com.wajam.nrv.consistency.ServiceMemberClusterStorage.CachedMemberChangeEvent

/**
 * Dummy dynamic cluster manager used to test ConsistencyMasterSlave
 */
class DummyDynamicClusterManager(members: Map[String, ServiceMemberClusterStorage]) extends DynamicClusterManager {

  private var initialized = false

  members.values.foreach(_.addObserver(handleServiceCacheChange))

  // ClusterManager trait implementation

  protected def initializeMembers() = {
    allServices.foreach(service => syncService(service))
    initialized = true
  }

  // DynamicClusterManager trait implementation

  protected def getServiceMembers(service: Service): Option[Seq[(ServiceMember, Seq[ServiceMemberVote])]] = {
    members.get(service.name).map(_.toIterator.map { member =>
      val copy = new ServiceMember(member.token, member.node, member.status)
      copy -> Seq(new ServiceMemberVote(copy, copy, copy.status))
    }.toList)
  }

  protected def removingOldServiceMember(service: Service, oldServiceMember: ServiceMember) = {
    trace(s"removingOldServiceMember: $service, $oldServiceMember")
  }

  protected def addingNewServiceMember(service: Service, newServiceMember: ServiceMember) = {
    trace(s"addingNewServiceMember: $service, $newServiceMember")
  }

  protected def voteServiceMemberStatus(service: Service, vote: ServiceMemberVote) = {
    val member = new ServiceMember(vote.candidateMember.token, vote.candidateMember.node, vote.statusVote)
    members(service.name).addMember(member)
  }

  private def handleServiceCacheChange(event: Event): Unit = {
    if (initialized) {
      event match {
        case e: CachedMemberChangeEvent => {
          // Force service check in a different thread
          import ExecutionContext.Implicits.global
          Future {
            info(s"Force service check after receiving $e")
            forceServiceCheck(serviceByName(e.serviceName))
          }
        }
        case _ =>
      }
    }
  }

  private def serviceByName(name: String) = allServices.find(_.name == name).get
}

/**
 * Store the cluster-wide topology of a service i.e. simplified substitute of Zookeeper for testing purposes
 */
class ServiceMemberClusterStorage(val serviceName: String) extends Observable with Logging {

  @volatile
  private var members = Map[Long, (Node, MemberStatus)]()

  def toIterator: Iterator[ServiceMember] = members.toIterator.map { case (t, (n, s)) => new ServiceMember(t, n, s) }

  def addMember(member: ServiceMember): Unit = {
    synchronized(members += member.token ->(member.node, member.status))
    notifyObservers(CachedMemberChangeEvent(serviceName, member))
  }

  def setMemberDown(token: Long): Unit = {
    val (node, _) = members(token)
    addMember(new ServiceMember(token, node, MemberStatus.Down))
  }
}

object ServiceMemberClusterStorage {

  case class CachedMemberChangeEvent(serviceName: String, member: ServiceMember) extends Event

}