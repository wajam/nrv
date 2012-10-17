package com.wajam.nrv.service

import org.scalatest.{BeforeAndAfter, FunSuite}
import org.scalatest.mock.MockitoSugar
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.mockito.Matchers._
import org.mockito.Mockito._
import com.wajam.nrv.data.{Message, MessageType, InMessage, OutMessage}
import com.wajam.nrv.utils.{Future, Promise}

@RunWith(classOf[JUnitRunner])
class TestSwitchboard extends FunSuite with MockitoSugar with BeforeAndAfter {

  var switchboard: Switchboard = null

  before {
    switchboard = new Switchboard
    switchboard.start()
  }

  after {
    switchboard.stop()
  }

  test("in-out matching") {
    val sync = Promise[OutMessage]

    val outMessage = new OutMessage()
    outMessage.sentTime = System.currentTimeMillis()
    outMessage.path = "/test"
    outMessage.token = 0
    switchboard.handleOutgoing(null, outMessage, _ => {
      val inMessage = new InMessage()
      inMessage.function = MessageType.FUNCTION_RESPONSE
      inMessage.rendezvousId = outMessage.rendezvousId
      inMessage.token = outMessage.token
      switchboard.handleIncoming(null, inMessage, Unit => {
        sync.success(inMessage.matchingOutMessage.get)
      })
    })

    assert(Future.blocking(sync.future, 100) != null)
  }

  test("timeout") {
    val mockAction = mock[Action]

    val outMessage = new OutMessage
    outMessage.sentTime = 0
    outMessage.timeoutTime = 20

    switchboard.handleOutgoing(mockAction, outMessage)
    switchboard.getTime = () => {
      100
    }
    switchboard.checkTimeout()

    verify(mockAction).generateResponseMessage(anyObject[Message], anyObject[Message])
    verify(mockAction).callIncomingHandlers(anyObject[InMessage])
  }

}
