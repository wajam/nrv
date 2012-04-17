package com.wajam.nrv.service

import org.scalatest.FunSuite
import org.scalatest.mock.MockitoSugar
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import com.wajam.nrv.data.{MessageType, InMessage, OutMessage}
import com.wajam.nrv.utils.Sync

@RunWith(classOf[JUnitRunner])
class TestSwitchboard extends FunSuite with MockitoSugar {

  test("routing") {
    val sync = new Sync[Option[Boolean]]

    var switchboard = new Switchboard
    switchboard.start()

    val outMessage = new OutMessage()
    outMessage.path = "/test"
    switchboard.keepOutgoing(outMessage)

    val inMessage = new InMessage()
    inMessage.function = MessageType.FUNCTION_RESPONSE
    inMessage.rendezvous = outMessage.rendezvous
    switchboard.matchIncoming(inMessage, (matchingMsg) => {
      sync.done(Some(true))
    })

    assert(sync.get(100) == Some(true))
  }

}
