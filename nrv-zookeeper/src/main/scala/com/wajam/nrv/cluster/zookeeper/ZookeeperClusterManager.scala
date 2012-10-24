package com.wajam.nrv.cluster.zookeeper

import com.wajam.nrv.cluster.{ServiceMemberVote, DynamicClusterManager}
import com.wajam.nrv.service.{ServiceMember, Service}
import com.wajam.nrv.cluster.zookeeper.ZookeeperClient._
import com.wajam.nrv.Logging
import org.apache.zookeeper.CreateMode

/**
 * Dynamic cluster manager that uses Zookeeper to keep a consistent view of the cluster among nodes. It creates
 * service members on startup, watch different corresponding nodes in Zookeeper and resynchronize members when
 * something changes in Zookeeper.
 */
class ZookeeperClusterManager(val zk: ZookeeperClient) extends DynamicClusterManager with Logging {

  import ZookeeperClusterManager._

  // watch global zookeeper events
  zk.addObserver {
    case ZookeeperDisconnected(original) => {
      error("Lost connection with Zookeeper. Pausing the cluster")
      this.forceClusterDown()
    }

    case ZookeeperExpired(original) => {
      error("Connection with Zookeeper expired. Pausing the cluster")
      this.forceClusterDown()
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

  protected def voteServiceMemberStatus(service: Service, vote: ServiceMemberVote) {
    info("Voting for member {} in service {} to {}", vote.candidateMember, service, vote.statusVote)

    try {
      val path = zkMemberVotePath(service.name, vote.candidateMember.token, vote.voterMember.token)
      if (zk.ensureExists(path, vote.toString, CreateMode.EPHEMERAL)) {
        zk.set(path, vote.toString)
      }
    } catch {
      case e: Exception => error("Got an exception voting for member {} in service {} to {}: {}", vote.candidateMember, service, vote.statusVote, e)
    }
  }

  private def getZkServiceMembers(service: Service, watch: Boolean): Seq[ServiceMember] = {
    debug("Getting service members for service {}", service)

    val callback = if (watch) Some((e: NodeChildrenChanged) => {
      debug("Service members within service {} changed", service)
      try {
        getZkServiceMembers(service, watch = true)
      } catch {
        case e: Exception =>
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
    debug("Getting votes for {} in service {}", serviceMember, service)

    val callback = if (watch) Some((e: NodeChildrenChanged) => {
      debug("Votes for {} in service {} changed", serviceMember, service)
      try {
        getZkMemberVotes(service, serviceMember, watch = true)
      } catch {
        case e: Exception =>
      }
      syncZk(service, watch = false)
    })
    else None

    zk.getChildren(zkMemberPath(service.name, serviceMember.token), callback).map(voteMember => {
      getZkMemberVote(service, serviceMember, voteMember.toLong, watch = watch)
    })
  }

  private def getZkMemberVote(service: Service, candidateMember: ServiceMember, voterToken: Long, watch: Boolean): ServiceMemberVote = {
    debug("Getting vote for member {} by {} in service {}", candidateMember, voterToken, service)

    val callback = if (watch) Some((e: NodeValueChanged) => {
      debug("Vote for member {} by {} in service {} changed", candidateMember, voterToken, service)
      try {
        getZkMemberVote(service, candidateMember, voterToken, watch = true)
      } catch {
        case e: Exception =>
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

