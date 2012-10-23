package com.wajam.nrv.cluster

import actors.Actor
import com.wajam.nrv.utils.Scheduler
import com.wajam.nrv.service.{Service, ServiceMember, MemberStatus}
import com.wajam.nrv.Logging

/**
 * Manager of a cluster in which nodes can be added/removed and can go up and down.
 */
abstract class DynamicClusterManager extends ClusterManager with Logging {
  private var started = false
  private val CLUSTER_CHECK_IN_MS = 1000
  private val CLUSTER_PRINT_IN_MS = 5000

  override def start() {
    super.start()
    started = true
    EventLoop.start()
  }

  override def stop() {
    super.stop()
    EventLoop.stop()
  }

  def forceClusterCheck() {
    EventLoop !? CheckCluster
  }

  def forceClusterDown() {
    EventLoop !? ForceDown
  }

  protected def compileVotes(candidateMember: ServiceMember, votes: Seq[ServiceMemberVote]): MemberStatus = {
    // TODO: implement consensus, not just take the member vote
    val optSelfVote = votes.find(_.voterMember.token == candidateMember.token)
    optSelfVote match {
      case Some(vote) => vote.statusVote
      case None => MemberStatus.Down
    }
  }

  protected def syncServiceMembers(service: Service, members: Seq[(ServiceMember, Seq[ServiceMemberVote])]) {
    if (!started)
      syncServiceMembersImpl(service, members)
    else
      EventLoop !? SyncServiceMembers(service, members)
  }

  private def syncServiceMembersImpl(service: Service, members: Seq[(ServiceMember, Seq[ServiceMemberVote])]) {
    debug("Syncing service members of {}", service)

    val currentMembers = service.members
    val newMembers = members.map(_._1)
    val newMemberVotes = members.toMap

    val added = newMembers.toList filterNot currentMembers.toList.contains
    val removed = currentMembers.toList filterNot newMembers.toList.contains

    // add new members
    added.foreach(newMember => {
      info("New member {} in service {}", newMember, service)
      val votedStatus = compileVotes(newMember, newMemberVotes(newMember))
      newMember.setStatus(votedStatus, triggerEvent = true)
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
        currentMember.setStatus(votedStatus, triggerEvent = true).map(event =>
          info("Member {} of service {} changed status to {}", currentMember, service, votedStatus)
        )
      })
    })
  }

  protected def voteServiceMemberStatus(service: Service, vote: ServiceMemberVote)

  // Event loop events
  private sealed class ClusterEvent

  private case object ForceDown extends ClusterEvent

  private case object CheckCluster extends ClusterEvent

  private case object PrintCluster extends ClusterEvent

  private case class SyncServiceMembers(service: Service, members: Seq[(ServiceMember, Seq[ServiceMemberVote])]) extends ClusterEvent

  object EventLoop extends Actor {

    val checkScheduler = new Scheduler(this, CheckCluster, CLUSTER_CHECK_IN_MS, CLUSTER_CHECK_IN_MS, blockingMessage = true, autoStart = false)
    val printScheduler = new Scheduler(this, PrintCluster, CLUSTER_PRINT_IN_MS, CLUSTER_PRINT_IN_MS, blockingMessage = true, autoStart = false)

    override def start(): Actor = {
      printScheduler.start()
      checkScheduler.start()
      super.start()
    }

    def stop() {
      printScheduler.cancel()
      checkScheduler.cancel()
    }

    private def tryChangeLocalServiceMemberStatus(service: Service, member: ServiceMember, newStatus: MemberStatus) {
      info("Trying to switch status of local member {} in service {} to {}", member, service, newStatus)
      member.trySetStatus(newStatus) match {
        case Some(event) =>
          if (event.noVotes > 0) {
            info("Attempt to switch status of {} in service {} to {} failed because (yea={}, no={})",
              member, service, newStatus, event.yeaVotes, event.noVotes)
          }

          voteServiceMemberStatus(service, new ServiceMemberVote(member, member, newStatus))

        case None =>
          info("Status of {} in service {} was canceled (new={}, current={})", member, service, newStatus, member.status)
      }
    }

    def act() {
      loop {
        try {
          react {

            case PrintCluster =>
              allServices.foreach(service => info("\nLocal node: {}\n{}", cluster.localNode, service.printService))
              sender ! true

            case CheckCluster =>
              val members = allMembers
              debug("Checking cluster for any pending changes ({} members)", members.size)

              members.foreach {
                case (service, member) =>
                  debug("Checking member {} in service {} with current status {}", member, service, member.status)
                  member.status match {
                    case MemberStatus.Joining =>
                      if (cluster.isLocalNode(member.node)) {
                        tryChangeLocalServiceMemberStatus(service, member, MemberStatus.Up)
                      }

                    case MemberStatus.Down =>
                      if (cluster.isLocalNode(member.node)) {
                        tryChangeLocalServiceMemberStatus(service, member, MemberStatus.Joining)
                      }

                    case other =>
                    // don't do anything for the rest
                  }
              }
              sender ! true


            case ForceDown =>
              info("Forcing the whole cluster down")
              allMembers.foreach {
                case (service, member) => member.setStatus(MemberStatus.Down, triggerEvent = true)
              }
              sender ! true


            case SyncServiceMembers(service, members) =>
              syncServiceMembersImpl(service, members)
              sender ! true
          }
        } catch {
          case e: Exception => error("Got an exception in cluster manager event loop: {}", e)
        }
      }
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

