package com.wajam.nrv.consistency

import com.wajam.nrv.service.Action
import com.wajam.nrv.data.OutMessage
import com.wajam.nrv.Logging

/**
 * Consistency that only sends messages to one replica (the first online)
 */
class ConsistencyOne extends Consistency with Logging {
  override def handleOutgoing(action: Action, message: OutMessage) {
    val selected = message.destination.selectedReplicas

    // let only the first selected as selected
    selected.slice(1, selected.size).foreach(replica => replica.selected = false)
  }
}
