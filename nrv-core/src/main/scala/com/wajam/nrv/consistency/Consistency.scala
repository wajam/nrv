package com.wajam.nrv.consistency

import com.wajam.nrv.service.{ServiceMember, MessageHandler, Service}
import com.wajam.nrv.utils.{Observable, VotableEvent, Event}
import com.wajam.nrv.cluster.Cluster
import com.wajam.nrv.utils.timestamp.Timestamp
import com.wajam.nrv.data.Message

/**
 * Manage consistency inside the cluster based on events
 */
abstract class Consistency extends MessageHandler with Observable {

  var cluster: Cluster = null
  var bindedServices = List[Service]()

  def bindService(service: Service) {
    // keep track of cluster
    if (cluster == null) {
      cluster = service.cluster
    }

    if (!bindedServices.contains(service)) {
      bindedServices :+= service
      service.addObserver(this.serviceEvent)
      this.addParentObservable(service)
    }
  }

  def serviceEvent(event: Event) {
    event match {
      case ve: VotableEvent =>
        ve.vote(pass = true)
      case _ =>
    }
  }

  /**
   * Returns local service members with their associated consistency state. Service member without states
   * (e.g. member status Down) are excluded.
   */
  def localMembersStates: Iterable[(ResolvedServiceMember, MemberConsistencyState)]

  def start()

  def stop()
}

case class ConsistencyStateTransitionEvent(member: ServiceMember,
                                           from: Option[MemberConsistencyState],
                                           to: Option[MemberConsistencyState]) extends Event

sealed trait MemberConsistencyState

object MemberConsistencyState {

  def fromString(str:String): Option[MemberConsistencyState] = {
    str match {
      case Recovering.toString => Some(Recovering)
      case Ok.toString => Some(Ok)
      case Error.toString => Some(Error)
      case "None" => None
      case _ => throw new IllegalArgumentException("Unsupported MemberConsistencyState: %s".format(str))
    }
  }

  /**
   * The consistency manager is verifying the service member consistency and taking action to ensure the consistency.
   * Going into this state when the service member is joining the cluster. The service member status cannot be Up
   * while being in this consistency state.
   */
  object Recovering extends MemberConsistencyState {
    override val toString = "Recovering"
  }

  /**
   * The service member is consistent and member status can transition to Up from the consistency manager perspective.
   */
  object Ok extends MemberConsistencyState {
    override val toString = "Ok"
  }

  /**
   * There is a problem with the service member consistency. Stay in this state untill the service member status
   * goes Down.
   */
  object Error extends MemberConsistencyState {
    override val toString = "Error"
  }
}