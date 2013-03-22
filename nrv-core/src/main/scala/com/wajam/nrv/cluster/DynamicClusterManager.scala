package com.wajam.nrv.cluster

import actors.Actor
import com.wajam.nrv.utils.{TransformLogging, Scheduler}
import com.wajam.nrv.service.{StatusTransitionEvent, Service, ServiceMember, MemberStatus}
import com.wajam.nrv.Logging
import com.yammer.metrics.scala.Instrumented

/**
 * Manager of a cluster in which nodes can be added/removed and can go up and down. This manager uses
 * an actor to execute operations sequentially on the cluster. Concrete implementation of this class
 * uses the "syncServiceMembers" function to synchronise services members (addition/deletion/status change)
 */
abstract class DynamicClusterManager extends ClusterManager with Logging with Instrumented with TransformLogging {
  private val CLUSTER_CHECK_IN_MS = 1000
  private val CLUSTER_FORCESYNC_IN_MS = 7500
  private val CLUSTER_PRINT_IN_MS = 5000

  private val allServicesMetrics = new ServiceMetrics("all")
  private var servicesMetrics = Map[Service, ServiceMetrics]()

  // Prepend local node info to all log messages
  def transformLogMessage = (msg, params) => ("[local=%s] %s".format(cluster.localNode, msg), params)

  override def start() = {
    if (super.start()) {
      OperationLoop.start()
      true
    } else false
  }

  override def stop() = {
    if (super.stop()) {
      OperationLoop.stop()
      true
    } else false
  }

  def forceClusterCheck() {
    OperationLoop !? OperationLoop.CheckCluster
  }

  def forceClusterDown() {
    OperationLoop !? OperationLoop.ForceDown
  }

  protected def syncServiceMembers(service: Service, members: Seq[(ServiceMember, Seq[ServiceMemberVote])]) {
    // if cluster is not started, we sync directly without passing through actor
    if (!started || OperationLoop.forceSync)
      syncServiceMembersImpl(service, members)
    else
      OperationLoop !? OperationLoop.SyncServiceMembers(service, members)
  }

  protected def syncMembers()

  private def compileVotes(candidateMember: ServiceMember, votes: Seq[ServiceMemberVote]): MemberStatus = {
    // TODO: implement consensus, not just take the member vote
    val optSelfVote = votes.find(_.voterMember.token == candidateMember.token)
    optSelfVote match {
      case Some(vote) => vote.statusVote
      case None => MemberStatus.Down
    }
  }

  private def syncServiceMembersImpl(service: Service, members: Seq[(ServiceMember, Seq[ServiceMemberVote])]) {
    debug("Syncing service members of {}", service)

    val currentMembers = service.members.toSeq
    val newMembers = members.map(_._1)
    val newMemberVotes = members.toMap

    val added = newMembers diff currentMembers
    val removed = currentMembers diff newMembers

    // add new members
    added.foreach(newMember => {
      info("New member {} in service {}", newMember, service)
      val votedStatus = compileVotes(newMember, newMemberVotes(newMember))
      val event = newMember.setStatus(votedStatus, triggerEvent = true)
      updateStatusChangeMetrics(service, event)
      service.addMember(newMember)
    })

    // remove members
    removed.foreach(oldMember => {
      info("Member {} needs to be removed from service {}", oldMember, service)
      service.removeMember(oldMember)
    })

    // sync all members statuses
    newMembers.foreach(newMember => {
      service.getMemberAtToken(newMember.token).map(currentMember => {
        val votedStatus = compileVotes(currentMember, newMemberVotes(currentMember))
        currentMember.setStatus(votedStatus, triggerEvent = true).map(event => {
          info("Member {} of service {} changed status from {} to {}", currentMember, service, event.from, event.to)
          updateStatusChangeMetrics(service, Some(event))
        })
      })
    })
  }

  protected def voteServiceMemberStatus(service: Service, vote: ServiceMemberVote)

  private class ServiceMetrics(name: String) {
    @volatile private var serviceMemberCount = 0L
    @volatile private var serviceMemberUpCount = 0L
    @volatile private var serviceMemberDownCount = 0L
    @volatile private var serviceMemberJoiningCount = 0L

    private val serviceMemberGauge = metrics.gauge("service-members", name) {
      serviceMemberCount
    }
    private val serviceMemberUpGauge = metrics.gauge("service-members-up", name) {
      serviceMemberUpCount
    }
    private val serviceMemberDownGauge = metrics.gauge("service-members-down", name) {
      serviceMemberDownCount
    }
    private val serviceMemberJoiningGauge = metrics.gauge("service-members-joining", name) {
      serviceMemberJoiningCount
    }

    def update(members: Iterable[ServiceMember]) {
      serviceMemberCount = members.size
      serviceMemberUpCount = members.count(_.status == MemberStatus.Up)
      serviceMemberDownCount = members.count(_.status == MemberStatus.Down)
      serviceMemberJoiningCount = members.count(_.status == MemberStatus.Joining)
    }

    lazy private val statusChangeUpMeter = metrics.meter(
      "status-change-up", "status-change-up", name)
    lazy private val statusChangeDownMeter = metrics.meter(
      "status-change-down", "status-change-down", name)
    lazy private val statusChangeJoiningMeter = metrics.meter(
      "status-change-joining", "status-change-joining", name)
    lazy private val statusChangeUnknownMeter = metrics.meter(
      "status-change-unknown", "status-change-unknown", name)

    def memberStatusChange(status: MemberStatus) {
      status match {
        case MemberStatus.Up => statusChangeUpMeter.mark()
        case MemberStatus.Down => statusChangeDownMeter.mark()
        case MemberStatus.Joining => statusChangeJoiningMeter.mark()
        case _ => statusChangeUnknownMeter.mark()
      }
    }
  }

  /**
   * This method is not thread safe and must always be called from a single thread
   */
  private def updateServicesMetrics() {
    allServicesMetrics.update(allMembers.map(_._2))
    for (service <- allServices) {
      getServiceMetrics(service).update(service.members)
    }
  }

  private def getServiceMetrics(service: Service): ServiceMetrics = {
    servicesMetrics.get(service) match {
      case Some(s) => s
      case None => {
        val stats = new ServiceMetrics(service.name)
        servicesMetrics += (service -> stats)
        stats
      }
    }
  }

  private def updateStatusChangeMetrics(service: Service, event: Option[StatusTransitionEvent]) {
    event.foreach(event => {
      if (cluster.isLocalNode(event.member.node)) {
        allServicesMetrics.memberStatusChange(event.to)
        getServiceMetrics(service).memberStatusChange(event.to)
      }
    })
  }

  /**
   * This actor receives operations and execute them sequentially on the cluster.
   */
  private object OperationLoop extends Actor {

    // forceSync is only updated in the actor but could be read from another thread
    @volatile
    private[cluster] var forceSync = false

    // Operations
    sealed class ClusterOperation

    case object ForceDown extends ClusterOperation

    case object CheckCluster extends ClusterOperation

    case object PrintCluster extends ClusterOperation with Logging {
      def print() {
        allServices.foreach(service => debug("\nLocal node: {}\n{}", cluster.localNode, service.printService))
      }
    }

    case object ForceSync extends ClusterOperation

    case class SyncServiceMembers(service: Service, members: Seq[(ServiceMember, Seq[ServiceMemberVote])]) extends ClusterOperation

    val checkScheduler = new Scheduler(this, CheckCluster, CLUSTER_CHECK_IN_MS, CLUSTER_CHECK_IN_MS, blockingMessage = true, autoStart = false)
    val syncScheduler = new Scheduler(this, ForceSync, CLUSTER_FORCESYNC_IN_MS, CLUSTER_FORCESYNC_IN_MS, blockingMessage = true, autoStart = false)
    val printScheduler = new Scheduler(this, PrintCluster, CLUSTER_PRINT_IN_MS, CLUSTER_PRINT_IN_MS, blockingMessage = true, autoStart = false)

    override def start(): Actor = {
      printScheduler.start()
      syncScheduler.start()
      checkScheduler.start()
      super.start()
    }

    def stop() {
      printScheduler.cancel()
      syncScheduler.cancel()
      checkScheduler.cancel()
    }

    private def tryChangeServiceMemberStatus(service: Service, member: ServiceMember, newStatus: MemberStatus) {
      info("Trying to switch status of member {} in service {} to {}", member, service, newStatus)
      member.trySetStatus(newStatus) match {
        case Some(event) =>
          if (event.nayVotes > 0) {
            info("Attempt to switch status of {} in service {} to {} failed by vote (yea={}, nay={})",
              member, service, newStatus, event.yeaVotes, event.nayVotes)
          } else {
            voteServiceMemberStatus(service, new ServiceMemberVote(member, member, newStatus))
          }

        case None =>
          info("Status of {} in service {} was canceled (new={}, current={})", member, service, newStatus, member.status)
      }
    }

    def act() {
      loop {
        react {

          case PrintCluster =>
            try {
              PrintCluster.print()
            } catch {
              case e: Exception => error("Got an exception when printing cluster: ", e)
            } finally {
              sender ! true
            }

          case CheckCluster => {
            // periodically executed, check local down nodes and try to promote them to better status
            try {
              val members = allMembers
              debug("Checking cluster for any pending changes ({} members)", members.size)

              members.foreach {
                case (service, member) =>
                  debug("Checking member {} in service {} with current status {}", member, service, member.status)

                  // TODO: this implement currently only check for local nodes that could be promoted. Eventually, this
                  // will check for "joining" nodes and promote them if votes from consistency managers are positives
                  member.status match {
                    case MemberStatus.Joining =>
                      if (cluster.isLocalNode(member.node)) {
                        tryChangeServiceMemberStatus(service, member, MemberStatus.Up)
                      }

                    case MemberStatus.Down =>
                      if (cluster.isLocalNode(member.node)) {
                        tryChangeServiceMemberStatus(service, member, MemberStatus.Joining)
                      }

                    case other =>
                    // don't do anything for the rest
                  }
              }

              // Update service statistics
              updateServicesMetrics()
            } catch {
              case e: Exception =>
                error("Got an exception when checking cluster: ", e)
            } finally {
              sender ! true
            }
          }

          case ForceSync => // periodically executed, force refresh of cluster nodes
            debug("Forcing cluster sync")
            try {
              forceSync = true
              syncMembers()
            } catch {
              case e: Exception =>
                error("Got an exception when forcing cluster sync: ", e)
            } finally {
              forceSync = false
              sender ! true
            }

          case SyncServiceMembers(service, members) => // synchronise received members in service (add/delete/status change)
            try {
              syncServiceMembersImpl(service, members)
            } catch {
              case e: Exception =>
                error("Got an exception when syncing service members: ", e)
            } finally {
              sender ! true
            }

          case ForceDown =>
            info("Forcing the whole cluster down")
            try {
              allMembers.foreach {
                case (service, member) => {
                  val event = member.setStatus(MemberStatus.Down, triggerEvent = true)
                  updateStatusChangeMetrics(service, event)
                }
              }
            } catch {
              case e: Exception =>
                error("Got an exception when forcing the cluster down: ", e)
            } finally {
              sender ! true
            }
        }
      }
    }

    override def exceptionHandler = {
      case e: Exception => error("Got an exception in cluster manager event loop: ", e)
    }
  }
}

class ServiceMemberVote(val candidateMember: ServiceMember, val voterMember: ServiceMember, val statusVote: MemberStatus) {
  override def toString: String = "%s|%s".format(voterMember, statusVote.toString)
}

object ServiceMemberVote {
  def fromString(candidateMember: ServiceMember, data: String): ServiceMemberVote = {
    val Array(strVoterData, strStatusVote) = data.split('|')
    val statusVote = MemberStatus.fromString(strStatusVote)
    val voterMember = ServiceMember.fromString(strVoterData)
    new ServiceMemberVote(candidateMember, voterMember, statusVote)
  }
}
