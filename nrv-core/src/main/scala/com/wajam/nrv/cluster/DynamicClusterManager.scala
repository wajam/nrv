package com.wajam.nrv.cluster

import scala.actors.Actor
import com.wajam.nrv.utils.{TransformLogging, Scheduler}
import com.wajam.nrv.service._
import com.wajam.nrv.Logging
import com.yammer.metrics.scala.Instrumented
import java.util.concurrent.{TimeUnit, CountDownLatch}

/**
 * Manager of a cluster in which nodes can be added/removed and can go up and down. This manager uses
 * an actor to execute operations sequentially on the cluster. Concrete implementation of this class
 * uses the "syncServiceMembers" function to synchronise services members (addition/deletion/status change)
 */
abstract class DynamicClusterManager extends ClusterManager with Logging with Instrumented with TransformLogging {
  private val allServicesMetrics = new ServiceMetrics("all")
  private var servicesMetrics = Map[Service, ServiceMetrics]()

  // Prepend local node info to all log messages
  def transformLogMessage = (msg, params) => ("[local=%s] %s".format(cluster.localNode, msg), params)

  override def start() = {
    synchronized {
      if (isLeaving) {
        throw new RuntimeException("Cannot start a leaving cluster!")
      }
      if (super.start()) {
        OperationLoop.start()
        true
      } else false
    }
  }

  override def stop() = {
    if (super.stop()) {
      OperationLoop.stop()
      true
    } else false
  }

  def trySetServiceMemberStatusDown(service: Service, member: ServiceMember) {
    OperationLoop ! OperationLoop.TrySetServiceMembersDown(service, member)
  }

  protected def forceServiceCheck(service: Service) {
    OperationLoop !? OperationLoop.SyncService(service)
  }

  protected def forceClusterDown() {
    OperationLoop !? OperationLoop.ForceDown
  }

  protected def syncService(service: Service) {
    getServiceMembers(service).foreach(syncServiceMembersImpl(service, _))
  }

  protected def getServiceMembers(service: Service): Option[Seq[(ServiceMember, Seq[ServiceMemberVote])]]

  // Latch synchronization barrier used to wait for all local service members to go down before closing the server
  private var leavingLatch: Option[CountDownLatch] = None

  private def isLeaving: Boolean = leavingLatch.isDefined

  def leave(timeout: Long) {
    synchronized {
      if (!isLeaving) {
        leavingLatch = new Some(new CountDownLatch(1))
        OperationLoop !? OperationLoop.SetMembersLeaving
        if (leavingLatch.get.await(timeout, TimeUnit.MILLISECONDS)) {
          info("Leave after all service members gone down.")
        } else {
          val remainingMembers = allMembers.collect {
            case (service, member) if cluster.isLocalNode(member.node) && member.status != MemberStatus.Down => {
              "%s:%d=%s".format(service.name, member.token, member.status)
            }
          }.mkString("<", ", ", ">")
          info("Leave but some service members are not down yet: {}", remainingMembers)
        }
      }
    }
  }

  private def compileVotes(candidateMember: ServiceMember, votes: Seq[ServiceMemberVote]): MemberStatus = {
    // TODO: implement consensus, not just take the member vote
    val optSelfVote = votes.find(_.voterMember == candidateMember) // find the ServiceMemberVote that votes for itself.
    optSelfVote match {
      case Some(vote) => vote.statusVote
      case None => MemberStatus.Down
    }
  }

  private def syncServiceMembersImpl(service: Service, members: Seq[(ServiceMember, Seq[ServiceMemberVote])]) {
    debug("Syncing service members of {}", service)

    val isServiceInitialized = !service.members.isEmpty

    val currentMembers = service.members.toSeq
    val newMembers = members.map(_._1)
    val newMemberVotes = members.toMap

    var added = newMembers diff currentMembers
    var removed = currentMembers diff newMembers

    // update member
    val updated = added.flatMap(addedMember => removed.find(_.token == addedMember.token) match {
      case None => None
      case Some(removedMember: ServiceMember) => Some((addedMember,removedMember))
    })
    if (!updated.isEmpty) {
      added = added diff updated.map(_._1)
      removed = removed diff updated.map(_._2)

      updated foreach {
        case (addedMember, removedMember) => {
          info("Member {} needs to be updated to {} in service {}", removedMember, addedMember, service)
          val eventRemove = removedMember.setStatus(MemberStatus.Down, triggerEvent = true)
          updateStatusChangeMetrics(service, eventRemove)
          service.updateMember(addedMember)
          removingOldServiceMember(service, removedMember)
          val votedStatus = compileVotes(addedMember, newMemberVotes(addedMember))
          val eventAdd = addedMember.setStatus(votedStatus, triggerEvent = true)
          updateStatusChangeMetrics(service, eventAdd)
        }
      }
    }

    // remove members
    removed.foreach(oldMember => {
      info("Member {} needs to be removed from service {}", oldMember, service)
      removingOldServiceMember(service, oldMember)
      val event = oldMember.setStatus(MemberStatus.Down, triggerEvent = true)
      service.removeMember(oldMember)
      updateStatusChangeMetrics(service, event)
    })

    // add new members
    added.foreach(newMember => {
      info("New member {} in service {}", newMember, service)
      addingNewServiceMember(service, newMember)
      val votedStatus = compileVotes(newMember, newMemberVotes(newMember))
      val event = newMember.setStatus(votedStatus, triggerEvent = false) // an event wouldn't notify the service, as it is not part of it yet.
      service.addMember(newMember, triggerEvent = isServiceInitialized)
      updateStatusChangeMetrics(service, event)
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

  protected def removingOldServiceMember(service: Service, oldServiceMember: ServiceMember)

  protected def addingNewServiceMember(service: Service, newServiceMember: ServiceMember)

  protected def voteServiceMemberStatus(service: Service, vote: ServiceMemberVote)

  /**
   * This actor receives operations and execute them sequentially on the cluster.
   */
  private object OperationLoop extends Actor {

    private val ClusterTransitionInMs = 1000
    private val ClusterSyncInMs = 7500
    private val ClusterPrintInMs = 5000

    // Operations
    sealed class ClusterOperation

    case object ForceDown extends ClusterOperation

    case object SetMembersLeaving extends ClusterOperation

    case object ClusterTransition extends ClusterOperation

    case object ClusterPrint extends ClusterOperation with Logging {
      def print() {
        allServices.foreach(service => debug("\nLocal node: {}\n{}", cluster.localNode, service.printService))
      }
    }

    case object ClusterSync extends ClusterOperation

    case class SyncService(service: Service) extends ClusterOperation

    case class TrySetServiceMembersDown(service: Service, member: ServiceMember) extends ClusterOperation

    val transitionScheduler = new Scheduler(this, ClusterTransition, ClusterTransitionInMs, ClusterTransitionInMs,
      blockingMessage = true, autoStart = false, name = Some("DynamicClusterManager.ClusterTransition"))
    val syncScheduler = new Scheduler(this, ClusterSync, ClusterSyncInMs, ClusterSyncInMs,
      blockingMessage = true, autoStart = false, name = Some("DynamicClusterManager.ClusterSync"))
    val printScheduler = new Scheduler(this, ClusterPrint, ClusterPrintInMs, ClusterPrintInMs,
      blockingMessage = true, autoStart = false, name = Some("DynamicClusterManager.ClusterPrint"))

    override def start(): Actor = {
      printScheduler.start()
      syncScheduler.start()
      transitionScheduler.start()
      super.start()
    }

    def stop() {
      printScheduler.cancel()
      syncScheduler.cancel()
      transitionScheduler.cancel()
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

          case ClusterPrint =>
            try {
              ClusterPrint.print()
            } catch {
              case e: Exception => error("Got an exception when printing cluster: ", e)
            } finally {
              sender ! true
            }

          case ClusterTransition => {
            // periodically executed, check local down nodes and try to promote them to better status
            try {
              val members = allMembers
              debug("Checking cluster for any pending changes ({} members)", members.size)

              members.foreach {
                case (service, member) if cluster.isLocalNode(member.node) =>
                  debug("Checking member {} in service {} with current status {}", member, service, member.status)

                  // TODO: this implement currently only check for local nodes that could be promoted. Eventually, this
                  // will check for "joining" nodes and promote them if votes from consistency managers are positives
                  member.status match {
                    case MemberStatus.Joining => tryChangeServiceMemberStatus(service, member, MemberStatus.Up)
                    case MemberStatus.Down if !isLeaving => tryChangeServiceMemberStatus(service, member, MemberStatus.Joining)
                    case MemberStatus.Leaving => tryChangeServiceMemberStatus(service, member, MemberStatus.Down)
                    case _ => // don't do anything for other types of MemberStatus
                  }
                case _ =>
              }

              // This will only be called if a latch exists, meaning the cluster is currently attempting to leave
              leavingLatch match {
                case Some(latch) if latch.getCount > 0 => {
                  if (allMembers.count({
                    case (_, member) => cluster.isLocalNode(member.node) && member.status != MemberStatus.Down
                  }) == 0) {
                    latch.countDown()
                  }
                }
                case _ =>
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

          case TrySetServiceMembersDown(service, member) => {
            info("Try set member {} down for service {}", member, service)
            try {
              tryChangeServiceMemberStatus(service, member, MemberStatus.Down)
            } catch {
              case e: Exception =>
                error("Got an exception when trying to set member {} for service {} down: ", member, service, e)
            }
          }

          case ClusterSync => {
            // Periodically executed to refresh the entire cluster
            debug("Forcing cluster sync")
            try {
              allServices.foreach(service => syncService(service))
            } catch {
              case e: Exception =>
                error("Got an exception when forcing cluster sync: ", e)
            } finally {
              sender ! true
            }
          }

          case SyncService(service) => {
            // Synchronise the members (i.e. add/delete/status change) of the received service
            try {
              syncService(service)
            } catch {
              case e: Exception =>
                error("Got an exception when syncing service members: ", e)
            } finally {
              sender ! true
            }
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

          case SetMembersLeaving =>
            info("Preparing for shutdown, setting all local nodes to Leaving status")
            try {
              allMembers.foreach {
                case (service, member) => if (cluster.isLocalNode(member.node)) {
                  val event = member.setStatus(MemberStatus.Leaving, triggerEvent = true)
                  voteServiceMemberStatus(service, new ServiceMemberVote(member, member, MemberStatus.Leaving))
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

  private class ServiceMetrics(name: String) {
    //gauge metrics for all services
    @volatile private var allServiceMemberCount = 0L
    @volatile private var allServiceMemberUpCount = 0L
    @volatile private var allServiceMemberDownCount = 0L
    @volatile private var allServiceMemberJoiningCount = 0L

    private val allServiceMemberGauge = metrics.gauge("all-service-members", name) {
      allServiceMemberCount
    }
    private val allServiceMemberUpGauge = metrics.gauge("all-service-members-up", name) {
      allServiceMemberUpCount
    }
    private val allServiceMemberDownGauge = metrics.gauge("all-service-members-down", name) {
      allServiceMemberDownCount
    }
    private val allServiceMemberJoiningGauge = metrics.gauge("all-service-members-joining", name) {
      allServiceMemberJoiningCount
    }
    //gauge metrics for local services
    @volatile private var localServiceMemberCount = 0L
    @volatile private var localServiceMemberUpCount = 0L
    @volatile private var localServiceMemberDownCount = 0L
    @volatile private var localServiceMemberJoiningCount = 0L

    private val localServiceMemberGauge = metrics.gauge("local-service-members", name) {
      localServiceMemberCount
    }
    private val localServiceMemberUpGauge = metrics.gauge("local-service-members-up", name) {
      localServiceMemberUpCount
    }
    private val localServiceMemberDownGauge = metrics.gauge("local-service-members-down", name) {
      localServiceMemberDownCount
    }
    private val localServiceMemberJoiningGauge = metrics.gauge("local-service-members-joining", name) {
      localServiceMemberJoiningCount
    }

    def update(members: Iterable[ServiceMember]) {
      //update metrics for all nodes
      allServiceMemberCount = members.size
      allServiceMemberUpCount = members.count(_.status == MemberStatus.Up)
      allServiceMemberDownCount = members.count(_.status == MemberStatus.Down)
      allServiceMemberJoiningCount = members.count(_.status == MemberStatus.Joining)

      //update metrics for local nodes
      val localMembers = members.filter(m => cluster.isLocalNode(m.node))
      localServiceMemberCount = localMembers.size
      localServiceMemberUpCount = localMembers.count(_.status == MemberStatus.Up)
      localServiceMemberDownCount = localMembers.count(_.status == MemberStatus.Down)
      localServiceMemberJoiningCount = localMembers.count(_.status == MemberStatus.Joining)
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

  private def updateStatusChangeMetrics(service: Service, eventOpt: Option[StatusTransitionEvent]) {
    eventOpt match {
      case Some(event) if cluster.isLocalNode(event.member.node) => {
        allServicesMetrics.memberStatusChange(event.to)
        getServiceMetrics(service).memberStatusChange(event.to)
      }
      case _ =>
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
