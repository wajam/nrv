package com.wajam.nrv.consistency.replication

import com.wajam.nrv.service.{ActionSupportOptions, Action, ActionPath}
import org.mockito.Mockito
import com.wajam.nrv.data.{OutMessage, InMessage}
import org.scalatest.mock.MockitoSugar

class ActionProxy(path: ActionPath, responseTimeout: Long)
  extends Action(path, (message: InMessage) => {}, actionSupportOptions =
    new ActionSupportOptions(responseTimeout = Some(responseTimeout))) with MockitoSugar {

  val mockAction: Action = mock[Action]

  override def checkSupported() {
    // IMPORTANT: do not check support, this is not used in a started cluster
  }

  override protected[nrv] def callOutgoingHandlers(outMessage: OutMessage) {
    mockAction.callOutgoingHandlers(outMessage)
  }

  override protected[nrv] def callIncomingHandlers(fromMessage: InMessage) {
    mockAction.callIncomingHandlers(fromMessage)
  }

  def verifyZeroInteractions(wait: Long = 0L) {
    Thread.sleep(wait)
    Mockito.verifyZeroInteractions(mockAction)
  }

  def verifyNoMoreInteractions(wait: Long = 0L) {
    Thread.sleep(wait)
    Mockito.verifyNoMoreInteractions(mockAction)
  }
}

object ActionProxy {
  def apply() = new ActionProxy(new ActionPath(""), responseTimeout = 1000L)
}
