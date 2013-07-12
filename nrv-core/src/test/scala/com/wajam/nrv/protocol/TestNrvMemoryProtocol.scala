package com.wajam.nrv.protocol

import org.scalatest.FunSuite
import com.wajam.nrv.cluster.LocalNode
import com.wajam.nrv.data.{Message, OutMessage}
import org.mockito.Mockito._
import org.mockito.Matchers._
import org.scalatest.matchers.ShouldMatchers
import com.wajam.nrv.utils.test.FunctionalMatcher

class TestNrvMemoryProtocol  extends FunSuite with ShouldMatchers  {

  test("can send-receive a message") {

    val node = new LocalNode("127.0.0.1", Map("nrv" -> 12345))

    val nmp = spy(new NrvMemoryProtocol("test", node))

    // Stop execution at handleIncoming
    doNothing().when(nmp).handleIncoming(anyObject(), anyObject())

    nmp.start()

    val message = new OutMessage()

    message.messageData = "Test"

    nmp.sendMessage(node, message, false, flags = Map(), (result: Option[Throwable]) => {
      result should be(None)
    })

    verify(nmp, timeout(100)).handleIncoming(anyObject(), argThat(new FunctionalMatcher((msg: Message) =>  message.messageData == msg.messageData)))
  }

  test("can send-receive a response") {

    val node = new LocalNode("127.0.0.1", Map("nrv" -> 12345))

    val nmp = spy(new NrvMemoryProtocol("test", node))

    // Stop execution at handleIncoming
    doNothing().when(nmp).handleIncoming(anyObject(), anyObject())

    nmp.start()

    val message = new OutMessage()

    message.messageData = "Test"

    nmp.sendResponse(node, message, false, flags = Map(), (result: Option[Throwable]) => {
      result should be(None)
    })

    verify(nmp, timeout(100)).handleIncoming(anyObject(), argThat(new FunctionalMatcher((msg: Message) =>  message.messageData == msg.messageData)))
  }
}
