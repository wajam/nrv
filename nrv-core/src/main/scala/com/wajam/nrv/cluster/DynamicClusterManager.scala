package com.wajam.nrv.cluster

import actors.Actor
import com.wajam.nrv.utils.Scheduler
import com.wajam.nrv.service.{Service, ServiceMember, MemberStatus}
import com.wajam.nrv.Logging

/**
 * Manager of a cluster in which nodes can be added/removed and can goes up and down.
 */
abstract class DynamicClusterManager extends ClusterManager with Logging {

  private var started = false
  private val CLUSTER_CHECK_IN_MS = 1000

  override def start() {
    super.start()
    started = true
    EventLoop.start()
  }

  def forceClusterCheck() {
    EventLoop ! CheckCluster
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
    info("Syncing service members of {}", service)

    val currentMembers = service.members
    val newMembers = members.map(_._1)

    val memberVotes = members.toMap
    val added = newMembers.toList filterNot currentMembers.toList.contains
    val removed = currentMembers.toList filterNot newMembers.toList.contains

    // add new members
    added.foreach(member => {
      info("New member in service {}: {}, {} {}", service, member, currentMembers.toList.contains(member))
      val votedStatus = compileVotes(member, memberVotes(member))
      member.setStatus(votedStatus, triggerEvent = true)
      addMember(service, member)
    })

    // TODO: support removed

    // sync all members statuses
    newMembers.foreach(member => {
      service.getMemberAtToken(member.token).map(currentMember => {
        val votedStatus = compileVotes(member, memberVotes(member))
        currentMember.setStatus(votedStatus, triggerEvent = true).map(event =>
          info("Member {} changed status to {}", member, votedStatus)
        )
      })
    })

  }

  // Event loop events
  private sealed class ClusterEvent

  private case object CheckCluster extends ClusterEvent

  private case class SyncServiceMembers(service: Service, members: Seq[(ServiceMember, Seq[ServiceMemberVote])]) extends ClusterEvent

  object EventLoop extends Actor {

    val schdlr = new Scheduler(this, CheckCluster, CLUSTER_CHECK_IN_MS, CLUSTER_CHECK_IN_MS, blockingMessage = true, autoStart = false)

    override def start(): Actor = {
      schdlr.start()
      super.start()
    }

    def act() {
      loop {
        react {
          case CheckCluster =>

          case SyncServiceMembers(service, members) =>
            syncServiceMembersImpl(service, members)
            sender ! true
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

