package com.wajam.nrv.zookeeper.cluster

import com.wajam.nrv.cluster.{ServiceMemberVote, DynamicClusterManager}
import com.wajam.nrv.service.{MemberStatus, ServiceMember, Service}
import com.wajam.nrv.zookeeper.ZookeeperClient
import com.wajam.nrv.zookeeper.ZookeeperClient._
import com.wajam.nrv.Logging
import org.apache.zookeeper.CreateMode
import com.wajam.nrv.zookeeper.service.ZookeeperService
import collection.mutable
import org.apache.zookeeper.KeeperException.NoNodeException

/**
 * Dynamic cluster manager that uses Zookeeper to keep a consistent view of the cluster among nodes. It creates
 * service members on startup, watch different corresponding nodes in Zookeeper and resynchronize members when
 * something changes in Zookeeper.
 */
class ZookeeperClusterManager(val zk: ZookeeperClient) extends DynamicClusterManager with Logging {

  import ZookeeperClusterManager._

  private val dataWatches = new mutable.HashMap[String, Function1[NodeValueChanged, Unit]] with
    mutable.SynchronizedMap[String, Function1[NodeValueChanged, Unit]]
  private val childWatches = new mutable.HashMap[String, Function1[NodeChildrenChanged, Unit]] with
    mutable.SynchronizedMap[String, Function1[NodeChildrenChanged, Unit]]

  override def start(): Boolean = {
    if (super.start()) {
      syncServices()

      // watch global zookeeper events
      addZkObserver()

      true
    } else false
  }

  private def addZkObserver() {
    zk.addObserver {
      case ZookeeperConnected(original) => {
        info("Connection to zookeeper established")
        syncServices()
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
    syncServices()
  }

  private def connected = {
    started && zk.connected
  }

  protected def syncMembers() {
    if (connected) {
      syncServices()
    }
  }

  private def syncServices() {
    allServices.foreach(service => syncService(service))
  }

  private def syncService(service: Service) {
    if (connected) {
      syncServiceMembers(service, getZkServiceMembers(service).map(serviceMember => {
        val votes = getZkMemberVotes(service, serviceMember)
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

  private def getZkServiceMembers(service: Service): Seq[ServiceMember] = {
    debug("Getting service members for service {}", service)

    val path = ZookeeperService.membersPath(service.name)
    val callback = childWatches.getOrElseUpdate(path, (e: NodeChildrenChanged) => {
      if (connected) {
        debug("Service members within service {} changed", service)
        syncService(service)
      }
    })

    zk.getChildren(path, Some(callback)).map(token => {
      val data = zk.getString(zkMemberPath(service.name, token.toLong))
      ServiceMember.fromString(data)
    })
  }

  private def getZkMemberVotes(service: Service, serviceMember: ServiceMember): Seq[ServiceMemberVote] = {
    debug("Getting votes for {} in service {}", serviceMember, service)

    val path = zkMemberVotesPath(service.name, serviceMember.token)
    val callback = childWatches.getOrElseUpdate(path, (e: NodeChildrenChanged) => {
      if (connected) {
        debug("Votes for {} in service {} changed", serviceMember, service)
        syncService(service)
      }
    })

    zk.getChildren(path, Some(callback)).map(voteMember => {
      getZkMemberVote(service, serviceMember, voteMember.toLong)
    })
  }

  private def getZkMemberVote(service: Service, candidateMember: ServiceMember, voterToken: Long): ServiceMemberVote = {
    debug("Getting vote for member {} by {} in service {}", candidateMember, voterToken, service)

    val path = zkMemberVotePath(service.name, candidateMember.token, voterToken)
    val callback = dataWatches.getOrElseUpdate(path, (e: NodeValueChanged) => {
      if (connected) {
        info("Vote for member {} by {} in service {} changed", candidateMember, voterToken, service)
        syncService(service)
      }
    })

    val data = zk.getString(path, Some(callback))
    ServiceMemberVote.fromString(candidateMember, data)
  }

  protected def removeOldServiceMember(service: Service, oldServiceMember: ServiceMember) {
    if (cluster.isLocalNode(oldServiceMember.node)) {
      //removing the memeber's own vote here, allowing the member's status to change
      //TODO: we assume the node is voting for itself, this may change when consensus is implemented.
      try {
        val path = ZookeeperClusterManager.zkMemberVotePath(service.name, oldServiceMember.token, oldServiceMember.token)
        zk.delete(path)
      } catch {
        case e: NoNodeException => //data has already been deleted (e.g. entries are deleted manually in ZKclusterManager it tests)
      }
    }
  }

  protected def addNewServiceMember(service: Service, newServiceMember: ServiceMember) {}

}

object ZookeeperClusterManager {
  private[zookeeper] def zkServicePath(serviceName: String) = ZookeeperService.path(serviceName)

  private[zookeeper] def zkMemberPath(serviceName: String, token: Long) = ZookeeperService.memberPath(serviceName, token)

  private[zookeeper] def zkMemberVotesPath(serviceName: String, candidateToken: Long) =
    ZookeeperService.memberPath(serviceName, candidateToken) + "/votes"

  private[zookeeper] def zkMemberVotePath(serviceName: String, candidateToken: Long, voterToken: Long) =
    ZookeeperService.memberPath(serviceName, candidateToken) + "/votes/" + voterToken
}

