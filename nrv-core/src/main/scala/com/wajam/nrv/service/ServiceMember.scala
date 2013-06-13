package com.wajam.nrv.service

import com.wajam.nrv.cluster.Node
import java.io.Serializable
import com.wajam.nrv.utils.{VotableEvent, Event, Observable}

/**
 * Node that is member of a service, at a specific position (token) in
 * the consistent hashing ring of the service.
 */
sealed class ServiceMember(val token: Long,
                           val node: Node,
                           protected var _status: MemberStatus = MemberStatus.Down)
  extends Serializable with Observable {

  def status = this._status

  def isLegalStatusTransition(oldStatus: MemberStatus, newStatus: MemberStatus): Boolean = {
    (oldStatus, newStatus) match {
      case (MemberStatus.Down, MemberStatus.Joining) => true
      case (MemberStatus.Joining, MemberStatus.Up) => true
      case (MemberStatus.Up, MemberStatus.Leaving) => true
      case (MemberStatus.Up, MemberStatus.Down) => true
      case (MemberStatus.Leaving, MemberStatus.Down) => true
      case _ => false
    }
  }

  /**
   * This method checks if a status change is allowed by all observers observing the current service member.
   * A StatusTransitionAttemptEvent is created and sent to every observer. Each observer will vote directly on
   * the received event on whether they accept the requested status change or not. The event containing the vote
   * result is then returned.
   */
  private[nrv] def trySetStatus(newStatus: MemberStatus): Option[StatusTransitionAttemptEvent] = {
    if (this._status != newStatus) {
      val event = new StatusTransitionAttemptEvent(this, this._status, newStatus)
      this.notifyObservers(event)
      Some(event)
    } else {
      None
    }
  }

  private[nrv] def setStatus(newStatus: MemberStatus, triggerEvent: Boolean): Option[StatusTransitionEvent] = {
    if (this._status != newStatus) {
      val oldStatus = this._status
      this._status = newStatus

      if (triggerEvent) {
        val event = new StatusTransitionEvent(this, oldStatus, newStatus)
        this.notifyObservers(event)
        Some(event)
      } else {
        None
      }
    } else {
      None
    }
  }

  lazy val uniqueKey = "%d:%s".format(token, node.toString)

  override def hashCode(): Int = uniqueKey.hashCode

  override def equals(that: Any) = that match {
    case other: ServiceMember => this.uniqueKey.equalsIgnoreCase(other.uniqueKey)
    case _ => false
  }

  override def toString: String = "%d:%s".format(token, node.toString)
}

object ServiceMember {

  def fromString(memberString: String): ServiceMember = {
    val Array(strToken, strNode) = memberString.split(":", 2)
    new ServiceMember(strToken.toLong, Node.fromString(strNode))
  }

}


case class StatusTransitionAttemptEvent(member: ServiceMember, from: MemberStatus, to: MemberStatus) extends VotableEvent

case class StatusTransitionEvent(member: ServiceMember, from: MemberStatus, to: MemberStatus) extends Event

sealed trait MemberStatus extends Serializable

object MemberStatus {

  def fromString(name: String): MemberStatus = {
    name.toLowerCase match {
      case "down" => Down
      case "joining" => Joining
      case "up" => Up
      case "leaving" => Leaving
    }
  }

  case object Down extends MemberStatus {
    override def toString = "down"
  }

  case object Joining extends MemberStatus {
    override def toString = "joining"
  }

  case object Up extends MemberStatus {
    override def toString = "up"
  }

  case object Leaving extends MemberStatus {
    override def toString = "leaving"
  }

}


