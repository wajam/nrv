package com.wajam.nrv.service

import com.wajam.nrv.cluster.Node
import java.io.Serializable
import com.wajam.nrv.utils.{VotableEvent, Event, Observable}

/**
 * Node that is member of a service, at a specific position (token) in
 * the consistent hashing ring of the service.
 */
sealed class ServiceMember(private val service: Service,
                           val token: Long,
                           val node: Node,
                           protected var _status: MemberStatus = MemberStatus.Down)
  extends Serializable with Observable {

  def status = this._status

  private[nrv] def trySetStatus(newStatus: MemberStatus, triggerEvent: Boolean): Option[StatusTransitionAttemptEvent] = {
    if (this._status != newStatus) {
      if (triggerEvent) {
        val event = new StatusTransitionAttemptEvent(this, this._status, newStatus)
        this.notifyObservers(event)
        Some(event)
      } else {
        None
      }
    } else {
      None
    }
  }

  private[nrv] def setStatus(newStatus: MemberStatus, triggerEvent: Boolean): Option[StatusTransitionEvent] = {
    if (this._status != newStatus) {
      this._status = newStatus

      if (triggerEvent) {
        val event = new StatusTransitionEvent(this, this._status, newStatus)
        this.notifyObservers(event)
        Some(event)
      } else {
        None
      }
    } else {
      None
    }
  }

}

case class StatusTransitionAttemptEvent(member: ServiceMember, from: MemberStatus, to: MemberStatus) extends VotableEvent

case class StatusTransitionEvent(member: ServiceMember, from: MemberStatus, to: MemberStatus) extends Event

sealed trait MemberStatus extends Serializable;

object MemberStatus {

  case object Down extends MemberStatus

  case object Joining extends MemberStatus

  case object Up extends MemberStatus

  case object Leaving extends MemberStatus

}


