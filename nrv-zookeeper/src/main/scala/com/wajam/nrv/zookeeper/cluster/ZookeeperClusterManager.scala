package com.wajam.nrv.zookeeper.cluster

import com.wajam.nrv.cluster.{ServiceMemberVote, DynamicClusterManager}
import com.wajam.nrv.service.{ServiceMember, Service}
import com.wajam.nrv.zookeeper.ZookeeperClient
import com.wajam.nrv.zookeeper.ZookeeperClient._
import com.wajam.nrv.Logging
import org.apache.zookeeper.CreateMode
import com.wajam.nrv.zookeeper.service.ZookeeperService

/**
 * Dynamic cluster manager that uses Zookeeper to keep a consistent view of the cluster among nodes. It creates
 * service members on startup, watch different corresponding nodes in Zookeeper and resynchronize members when
 * something changes in Zookeeper.
 */
class ZookeeperClusterManager(val zk: ZookeeperClient) extends DynamicClusterManager with Logging {

  import ZookeeperClusterManager._

  override def start():Boolean = {
    if (super.start()) {
      syncServices(watch = true)

      // watch global zookeeper events
      addZkObserver()

      true
    } else false
  }

  private def addZkObserver() {
    zk.addObserver {
      case ZookeeperConnected(original) => {
        info("Connection to zookeeper established")
        syncServices(watch = true)
      }

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
  }

  protected def initializeMembers() {
    syncServices(watch = false)
  }

  private def connected = {
    started && zk.connected
  }

  protected def syncMembers() {
    if (connected) {
      syncServices(watch = false)
    }
  }

  private def syncServices(watch: Boolean) {
    allServices.foreach(service => syncService(service, watch = watch))
  }

  private def syncService(service: Service, watch: Boolean) {
    if (connected) {
      syncServiceMembers(service, getZkServiceMembers(service, watch = watch).map(serviceMember => {
        val votes = getZkMemberVotes(service, serviceMember, watch = watch)
        (serviceMember, votes)
      }))
    }
  }

  protected def voteServiceMemberStatus(service: Service, vote: ServiceMemberVote) {
    info("Voting for member {} in service {} to {}", vote.candidateMember, service, vote.statusVote)

    try {
      val path = zkMemberVotePath(service.name, vote.candidateMember.token, vote.voterMember.token)
      val created = zk.ensureExists(path, vote.toString, CreateMode.EPHEMERAL)
      if (!created) {
        zk.set(path, vote.toString)
      }
    } catch {
      case e: Exception => error("Got an exception voting for member {} in service {} to {}: {}", vote.candidateMember, service, vote.statusVote, e)
    }
  }

  private def getZkServiceMembers(service: Service, watch: Boolean): Seq[ServiceMember] = {
    debug("Getting service members for service {}", service)

    val callback = if (watch) Some((e: NodeChildrenChanged) => {
      if (connected) {
        debug("Service members within service {} changed", service)
        try {
          getZkServiceMembers(service, watch = true)
        } catch {
          case e: Exception => debug("Got an exception getting service members in service {}: {}", service, e)
        }
        syncService(service, watch = false)
      }
    })
    else None

    zk.getChildren(ZookeeperService.membersPath(service.name), callback).map(token => {
      val data = zk.getString(zkMemberPath(service.name, token.toLong))
      ServiceMember.fromString(data)
    })
  }

  private def getZkMemberVotes(service: Service, serviceMember: ServiceMember, watch: Boolean): Seq[ServiceMemberVote] = {
    debug("Getting votes for {} in service {}", serviceMember, service)

    val callback = if (watch) Some((e: NodeChildrenChanged) => {
      if (connected) {
        debug("Votes for {} in service {} changed", serviceMember, service)
        try {
          getZkMemberVotes(service, serviceMember, watch = true)
        } catch {
          case e: Exception => debug("Got an exception getting votes for {} in service {}: {}", serviceMember, service, e)
        }
        syncService(service, watch = false)
      }
    })
    else None

    zk.getChildren(zkMemberVotesPath(service.name, serviceMember.token), callback).map(voteMember => {
      getZkMemberVote(service, serviceMember, voteMember.toLong, watch = watch)
    })
  }

  private def getZkMemberVote(service: Service, candidateMember: ServiceMember, voterToken: Long, watch: Boolean): ServiceMemberVote = {
    debug("Getting vote for member {} by {} in service {}", candidateMember, voterToken, service)

    val callback = if (watch) Some((e: NodeValueChanged) => {
      if (connected) {
        debug("Vote for member {} by {} in service {} changed", candidateMember, voterToken, service)
        try {
          getZkMemberVote(service, candidateMember, voterToken, watch = true)
        } catch {
          case e: Exception => debug("Got an exception getting vote for member {} by {} in service {}: {}", candidateMember, voterToken, service, e)
        }
        syncService(service, watch = false)
      }
    })
    else None

    val data = zk.getString(zkMemberVotePath(service.name, candidateMember.token, voterToken), callback)
    ServiceMemberVote.fromString(candidateMember, data)
  }

}

object ZookeeperClusterManager {
  private[zookeeper] def zkServicePath(serviceName: String) = ZookeeperService.path(serviceName)

  private[zookeeper] def zkMemberPath(serviceName: String, token: Long) = ZookeeperService.memberPath(serviceName, token)

  private[zookeeper] def zkMemberVotesPath(serviceName: String, candidateToken: Long) =
    ZookeeperService.memberPath(serviceName, candidateToken) + "/votes"

  private[zookeeper] def zkMemberVotePath(serviceName: String, candidateToken: Long, voterToken: Long) =
    ZookeeperService.memberPath(serviceName, candidateToken) + "/votes/" + voterToken
}

