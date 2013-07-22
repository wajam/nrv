package com.wajam.nrv.consistency

import com.wajam.nrv.service.{ServiceMember, Action}
import com.wajam.nrv.data.OutMessage
import com.wajam.nrv.Logging

/**
 * Consistency that only sends messages to one replica (the first online)
 */
class ConsistencyOne extends Consistency with Logging {
  override def handleOutgoing(action: Action, message: OutMessage) {
    // Let only the first selected as selected
    message.destination.deselectAllReplicasButOne()
  }

  def localMembersStates: Iterable[(ResolvedServiceMember, MemberConsistencyState)] = Nil

  def start() {}

  def stop() {}
}
