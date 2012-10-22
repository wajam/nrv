package com.wajam.nrv.cluster.zookeeper

import com.wajam.nrv.cluster.{ServiceMemberVote, DynamicClusterManager}
import com.wajam.nrv.service.{ServiceMember, Service}
import com.wajam.nrv.cluster.zookeeper.ZookeeperClient.{ZookeeperExpired, ZookeeperDisconnected, NodeValueChanged, NodeChildrenChanged}
import com.wajam.nrv.Logging

/**
 * Zookeeper cluster manager
 */
class ZookeeperClusterManager(val zk: ZookeeperClient) extends DynamicClusterManager with Logging {

  import ZookeeperClusterManager._

  zk.addObserver {
    case ZookeeperDisconnected(original) => {
      error("Lost connection with Zookeeper. Pausing the cluster")
      // TODO: turn down all service members
    }

    case ZookeeperExpired(original) => {
      error("Connection with Zookeeper expired. Pausing the cluster")
      // TODO: turn down all service members
    }

    case _ =>
  }

  protected def initializeMembers() {
    allServices.foreach(service => syncZk(service, watch = true))
  }

  private def syncZk(service: Service, watch: Boolean) {
    syncServiceMembers(service, getZkServiceMembers(service, watch = watch).map(serviceMember => {
      val votes = getZkMemberVotes(service, serviceMember, watch = watch)
      (serviceMember, votes)
    }))
  }

  private def getZkServiceMembers(service: Service, watch: Boolean): Seq[ServiceMember] = {
    info("Getting service members for service {}", service)

    val callback = if (watch) Some((e: NodeChildrenChanged) => {
      info("Service members within service {} changed", service)
      try {
        getZkServiceMembers(service, watch = true)
      } catch {
        case _ =>
      }
      syncZk(service, watch = false)
    })
    else None

    zk.getChildren(zkServicePath(service.name), callback).map(token => {
      val data = zk.getString(zkMemberPath(service.name, token.toLong))
      ServiceMember.fromString(data)
    })
  }

  private def getZkMemberVotes(service: Service, serviceMember: ServiceMember, watch: Boolean): Seq[ServiceMemberVote] = {
    info("Getting votes for {} in service {}", serviceMember, service)

    val callback = if (watch) Some((e: NodeChildrenChanged) => {
      info("Votes for {} in service {} changed", serviceMember, service)
      try {
        getZkMemberVotes(service, serviceMember, watch = true)
      } catch {
        case _ =>
      }
      syncZk(service, watch = false)
    })
    else None

    zk.getChildren(zkMemberPath(service.name, serviceMember.token), callback).map(voteMember => {
      getZkMemberVote(service, serviceMember, voteMember.toLong, watch = watch)
    })
  }

  private def getZkMemberVote(service: Service, candidateMember: ServiceMember, voterToken: Long, watch: Boolean): ServiceMemberVote = {
    info("Getting vote for member {} by {} in service {}", candidateMember, voterToken, service)

    val callback = if (watch) Some((e: NodeValueChanged) => {
      info("Vote for member {} by {} in service {} changed", candidateMember, voterToken, service)
      try {
        getZkMemberVote(service, candidateMember, voterToken, watch = true)
      } catch {
        case _ =>
      }
      syncZk(service, watch = false)
    })
    else None

    val data = zk.getString(zkMemberVotePath(service.name, candidateMember.token, voterToken), callback)
    ServiceMemberVote.fromString(candidateMember, data)
  }

}

object ZookeeperClusterManager {
  private[zookeeper] def zkServicePath(serviceName: String) = "/%s".format(serviceName)

  private[zookeeper] def zkMemberPath(serviceName: String, token: Long) = "/%s/%d".format(serviceName, token)

  private[zookeeper] def zkMemberVotePath(serviceName: String, candidateToken: Long, voterToken: Long) = "/%s/%d/%d".format(serviceName, candidateToken, voterToken)
}

