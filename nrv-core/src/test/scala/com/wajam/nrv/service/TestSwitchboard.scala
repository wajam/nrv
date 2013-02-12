package com.wajam.nrv.service

import org.scalatest.{BeforeAndAfter, FunSuite}
import org.scalatest.mock.MockitoSugar
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.mockito.Matchers._
import org.mockito.Mockito._
import com.wajam.nrv.data.{Message, MessageType, InMessage, OutMessage}
import com.wajam.nrv.utils.{Future, Promise}
import org.mockito.ArgumentMatcher
import org.mockito.stubbing.Answer
import org.mockito.invocation.InvocationOnMock

@RunWith(classOf[JUnitRunner])
class TestSwitchboard extends FunSuite with MockitoSugar with BeforeAndAfter {

  var switchboard: Switchboard = null

  before {
    switchboard = new Switchboard(numExecutor = 1, banExpirationDuration = 2000)
    switchboard.start()
  }

  after {
    switchboard.stop()
  }

  class MessageCodeMatcher(code: Int) extends ArgumentMatcher {
    def matches(argument: Any) = {
      val message = argument.asInstanceOf[Message]
      message.code == code
    }
  }

  def matchMessageCode(code: Int) = new MessageCodeMatcher(code)

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

    val outMessage = new OutMessage(responseTimeout = 20)
    outMessage.sentTime = 0

    switchboard.handleOutgoing(mockAction, outMessage)
    switchboard.getTime = () => {
      100
    }
    switchboard.checkTimeout()

    verify(mockAction).generateResponseMessage(anyObject[Message], anyObject[Message])
    verify(mockAction).callIncomingHandlers(anyObject[InMessage])
  }

  test("overloaded") {
    // Setup mock action. Complete promise when minimum rejected count is reached
    val minRejectedCount = 10
    val donePromise = Promise[Boolean]
    val mockAction = mock[Action]
    @volatile var rejectCount = 0
    when(mockAction.callOutgoingHandlers(argThat(matchMessageCode(503)))).then(new Answer[Unit]() {
      def answer(invocation: InvocationOnMock) {
        rejectCount += 1
        if (rejectCount > minRejectedCount) {
          donePromise.trySuccess(true)
        }
      }
    })

    // Queue incomming messages with different tokens
    var token = 0
    while(!donePromise.future.isCompleted) {
      queueIncomingMessage(mockAction, token = token, delay = 50)
      token += 1
      Thread.sleep(10)
    }

    verify(mockAction, atLeast(rejectCount)).callOutgoingHandlers(argThat(matchMessageCode(503)))
    verify(mockAction, atLeast(rejectCount)).generateResponseMessage(anyObject(), anyObject())
   }

  test("banned token") {
    // Setup mock action. Complete promise as soon a reply message with code 429 (banned) is seen
    val bannedPromise = Promise[Boolean]
    val mockAction = mock[Action]
    @volatile var bannedCount = 0
    when(mockAction.callOutgoingHandlers(argThat(matchMessageCode(429)))).then(new Answer[Unit]() {
      def answer(invocation: InvocationOnMock) {
        bannedCount += 1
        bannedPromise.trySuccess(true)
      }
    })

    // Queue incomming messages on the same token until got a banned error reply
    while(!bannedPromise.future.isCompleted) {
      queueIncomingMessage(mockAction, token = 0, delay = 50)
      Thread.sleep(10)
    }

    verify(mockAction).callOutgoingHandlers(argThat(matchMessageCode(503)))
    verify(mockAction, times(bannedCount)).callOutgoingHandlers(argThat(matchMessageCode(429)))
    verify(mockAction, times(bannedCount + 1)).generateResponseMessage(anyObject(), anyObject())

    // Wait until ban is lifted and queue one more message which should not be banned anymore
    Thread.sleep(switchboard.banExpirationDuration)
    queueIncomingMessage(mockAction, token= 0, delay = 0)
    Thread.sleep(100) // Wait to ensure message is processed
    verifyNoMoreInteractions(mockAction)
   }

  def queueIncomingMessage(action: Action, token: Long, delay: Long) {
    val message = new InMessage()
    message.path = "/test"
    message.token = token
    switchboard.handleIncoming(action, message, _ => {
      Thread.sleep(delay)
    })
  }
}
