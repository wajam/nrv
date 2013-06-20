package com.wajam.nrv.service

import org.mockito.Mockito
import com.wajam.nrv.data.{OutMessage, InMessage}
import org.scalatest.mock.MockitoSugar

class ActionProxy(path: ActionPath, responseTimeout: Long, mockAction: Action)
  extends Action(path, (message: InMessage) => {}, actionSupportOptions =
    new ActionSupportOptions(responseTimeout = Some(responseTimeout))) with MockitoSugar {

  override def checkSupported() {
    // IMPORTANT: do not check support, this is not used in a started cluster
  }

  override protected[nrv] def callOutgoingHandlers(outMessage: OutMessage) {
    mockAction.callOutgoingHandlers(outMessage)
  }

  override protected[nrv] def callIncomingHandlers(fromMessage: InMessage) {
    mockAction.callIncomingHandlers(fromMessage)
  }
}

object ActionProxy extends MockitoSugar {
  def apply(mockAction: Action = mock[Action]) = new ActionProxy(new ActionPath(""), responseTimeout = 1000L, mockAction)

  def verifyZeroInteractionsAfter(wait: Long, mocks: AnyRef*) {
    Thread.sleep(wait)
    Mockito.verifyZeroInteractions(mocks:_*)
  }

  def verifyNoMoreInteractionsAfter(wait: Long, mocks: AnyRef*) {
    Thread.sleep(wait)
    Mockito.verifyNoMoreInteractions(mocks:_*)
  }

}
