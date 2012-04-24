package com.wajam.nrv.service

import org.scalatest.FunSuite
import org.scalatest.mock.MockitoSugar
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import com.wajam.nrv.utils.Sync
import org.mockito.Matchers._
import org.mockito.Mockito._
import com.wajam.nrv.data.{Message, MessageType, InMessage, OutMessage}

@RunWith(classOf[JUnitRunner])
class TestSwitchboard extends FunSuite with MockitoSugar {

  val switchboard = new Switchboard
  switchboard.start()

  test("in-out matching") {
    val sync = new Sync[OutMessage]

    val outMessage = new OutMessage()
    outMessage.sentTime = System.currentTimeMillis()
    outMessage.path = "/test"
    switchboard.handleOutgoing(null, outMessage, _ => {
      val inMessage = new InMessage()
      inMessage.function = MessageType.FUNCTION_RESPONSE
      inMessage.rendezvousId = outMessage.rendezvousId
      switchboard.handleIncoming(null, inMessage, Unit => {
        sync.done(inMessage.matchingOutMessage.get)
      })
    })

    assert(sync.get(100) != null)
  }

  test("timeout") {
    val mockAction = mock[Action]

    val outMessage = new OutMessage
    outMessage.sentTime = 0
    outMessage.timeoutTime = 20

    switchboard.handleOutgoing(mockAction, outMessage)
    switchboard.getTime = ()=>{ 100 }
    switchboard.checkTimeout()

    verify(mockAction).generateResponseMessage(anyObject[Message], anyObject[Message])
    verify(mockAction).callIncomingHandlers(anyObject[InMessage])
  }

}
