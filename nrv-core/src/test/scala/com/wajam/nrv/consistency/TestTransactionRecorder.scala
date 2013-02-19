package com.wajam.nrv.consistency

import com.wajam.nrv.service.{ActionSupportOptions, ServiceMember, Service}
import com.wajam.nrv.cluster.LocalNode

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.BeforeAndAfter
import org.mockito.Mockito._
import org.mockito.Matchers._
import com.wajam.nrv.data._
import org.mockito.ArgumentMatcher
import org.hamcrest.Description
import org.scalatest.matchers.ShouldMatchers._

import com.wajam.nrv.utils.ControlableCurrentTime
import org.scalatest.mock.MockitoSugar
import com.wajam.nrv.utils.timestamp.Timestamp
import com.yammer.metrics.scala.Meter
import com.yammer.metrics.Metrics
import java.util.concurrent.TimeUnit
import persistence.{TransactionEvent, TransactionLog, TestTransactionBase}
import scala.Some


@RunWith(classOf[JUnitRunner])
class TestTransactionRecorder extends TestTransactionBase with BeforeAndAfter with MockitoSugar {

  var consistencyErrorMeter: Meter = null
  val appendDelay = 1000L
  var service: Service = null
  var member: ServiceMember = null
  var mockTxLog: TransactionLog = null
  var recorder: TransactionRecorder with ControlableCurrentTime = null

  before {
    service = new Service("service", new ActionSupportOptions(responseTimeout = Some(20000L)))
    member = new ServiceMember(0, new LocalNode(Map("nrv" -> 1234)))

    mockTxLog = mock[TransactionLog]
    when(mockTxLog.getLastLoggedTimestamp).thenReturn(None)

    recorder = new TransactionRecorder(service, member, mockTxLog, appendDelay) with ControlableCurrentTime
    recorder.currentTime = 0
    recorder.start()
    verify(mockTxLog).getLastLoggedTimestamp // Ignore call done at recorder construction

    consistencyErrorMeter = new Meter(Metrics.defaultRegistry().newMeter(
      recorder.getClass, "consistency-error", "consistency-error", TimeUnit.MILLISECONDS))
  }

  after {
    consistencyErrorMeter = null
    recorder.kill()
    recorder = null
    mockTxLog = null
    member = null
    service = null
  }

  private class TransactionEventMatcher(message: Message, previous: Option[Timestamp]) extends ArgumentMatcher {
    val timestamp = Consistency.getMessageTimestamp(message).get
    val token = message.token

    def matches(argument: Any): Boolean = {
      val tx = argument.asInstanceOf[TransactionEvent]
      tx.timestamp == timestamp && tx.previous == previous && tx.token == token &&
        tx.message.parameters == message.parameters && tx.message.metadata == message.metadata &&
        tx.message.messageData == message.messageData
    }

    override def describeTo(description: Description) {
      description.appendValue(TransactionEvent(message, previous))
    }
  }

  private def matchTx(message: Message, previous: Option[Timestamp] = None) = {
    new TransactionEventMatcher(message, previous)
  }

  private def createRequestMessage(timestamp: Long, token: Long = 0): InMessage = {
    val request = new InMessage((Map(("ts" -> MLong(timestamp)), ("tk" -> MLong(token)))))
    request.function = MessageType.FUNCTION_CALL
    request.token = token
    request.serviceName = service.name
    Consistency.setMessageTimestamp(request, Timestamp(timestamp))
    request
  }

  private def createResponseMessage(request: InMessage, code: Int = 200, error: Option[Exception] = None) = {
    val response = new OutMessage()
    request.copyTo(response)
    response.function = MessageType.FUNCTION_RESPONSE
    response.code = code
    response.error = error
    response
  }

  test("tx should be pending until responded") {
    recorder.pendingSize should be(0)
    val request = createRequestMessage(timestamp = 0)
    recorder.handleMessage(request)
    recorder.checkPending()
    recorder.pendingSize should be(1)

    val response = createResponseMessage(request)
    recorder.handleMessage(response)
    recorder.advanceTime(appendDelay + 1000)
    recorder.checkPending()
    recorder.pendingSize should be(0)
  }

  test("successful transaction should be appended to log AFTER append delay") {
    val request = createRequestMessage(timestamp = 0)
    recorder.handleMessage(request)
    recorder.checkPending()

    recorder.handleMessage(createResponseMessage(request))
    recorder.advanceTime(appendDelay + 1000)
    recorder.checkPending()

    verify(mockTxLog).append(argThat(matchTx(request)))
    verify(mockTxLog, atLeast(0)).commit() // Ignore all commit calls
    verifyZeroInteractions(mockTxLog)
  }

  test("successful transaction should NOT be appended to log BEFORE append delay") {
    val request = createRequestMessage(timestamp = 0)
    recorder.handleMessage(request)
    recorder.checkPending()
    recorder.pendingSize should be(1)

    recorder.handleMessage(createResponseMessage(request))
    recorder.checkPending()
    recorder.pendingSize should be(1)

    verify(mockTxLog, atLeast(0)).commit() // Ignore all commit calls
    verifyZeroInteractions(mockTxLog)
  }

  test("successful transactions should be logged in timestamp order even if seen and responded out of order") {
    val request1 = createRequestMessage(timestamp = 1)
    val request2 = createRequestMessage(timestamp = 2)
    val request3 = createRequestMessage(timestamp = 3)
    val request4 = createRequestMessage(timestamp = 4)

    recorder.handleMessage(request3)
    recorder.handleMessage(request4)
    recorder.handleMessage(request1)
    recorder.handleMessage(request2)
    recorder.checkPending() // Ensure requests are processed by actor

    recorder.handleMessage(createResponseMessage(request3))
    recorder.handleMessage(createResponseMessage(request4))
    recorder.handleMessage(createResponseMessage(request2))
    recorder.advanceTime(appendDelay + 1000)
    recorder.checkPending()
    recorder.handleMessage(createResponseMessage(request1))
    recorder.checkPending()

    verify(mockTxLog).append(argThat(matchTx(request1)))
    verify(mockTxLog).append(argThat(matchTx(request2, Some(Timestamp(1)))))
    verify(mockTxLog).append(argThat(matchTx(request3, Some(Timestamp(2)))))
    verify(mockTxLog).append(argThat(matchTx(request4, Some(Timestamp(3)))))
    verify(mockTxLog, atLeast(0)).commit() // Ignore all commit calls
    verifyZeroInteractions(mockTxLog)
  }

  test("failed transaction (code) should not be appended to log") {
    // not logged + not pending anymore
    val request = createRequestMessage(timestamp = 0)
    recorder.handleMessage(request)
    recorder.checkPending()
    recorder.pendingSize should be(1)

    recorder.handleMessage(createResponseMessage(request, code = 500))
    recorder.checkPending()
    recorder.pendingSize should be(0)

    verify(mockTxLog, atLeast(0)).commit() // Ignore all commit calls
    verifyZeroInteractions(mockTxLog)
  }

  test("failed transaction (exception) should not be appended to log") {
    // not logged + not pending anymore
    val request = createRequestMessage(timestamp = 0)
    recorder.handleMessage(request)
    recorder.checkPending()
    recorder.pendingSize should be(1)

    recorder.handleMessage(createResponseMessage(request, error = Some(new Exception)))
    recorder.checkPending()
    recorder.pendingSize should be(0)

    verify(mockTxLog, atLeast(0)).commit() // Ignore all commit calls
    verifyZeroInteractions(mockTxLog)
  }

  ignore("timeout transaction should be truncated from storage") {
  }

  ignore("success response for non pending transaction should be truncated from storage") {
  }

  test("success response without timestamp should raise a consistency error") {
    val before = consistencyErrorMeter.count

    recorder.pendingSize should be(0)
    recorder.handleMessage(createResponseMessage(new InMessage()))
    recorder.checkPending()
    recorder.pendingSize should be(0)

    consistencyErrorMeter.count should be(before + 1)
    // TODO: verify service member status goes down

    verify(mockTxLog, atLeast(0)).commit() // Ignore all commit calls
    verifyZeroInteractions(mockTxLog)
  }

  test("error response for non pending transaction should be ignored") {
    // two cases: 1) no timestamp in response and 2) no matching timestamp
    val before = consistencyErrorMeter.count

    recorder.pendingSize should be(0)
    recorder.handleMessage(createResponseMessage(new InMessage(), code = 500)) // no timestamp
    recorder.handleMessage(createResponseMessage(
      createRequestMessage(timestamp = 0), code = 500)) // with timestamp, no pending match
    recorder.checkPending()
    recorder.pendingSize should be(0)

    consistencyErrorMeter.count should be(before)

    verify(mockTxLog, atLeast(0)).commit() // Ignore all commit calls
    verifyZeroInteractions(mockTxLog)
  }

  test("duplicate transaction should raise a consistency error") {
    val before = consistencyErrorMeter.count

    recorder.handleMessage(createRequestMessage(timestamp = 0))
    recorder.checkPending()
    recorder.pendingSize should be(1)

    // message with duplicate timestamp
    recorder.handleMessage(createRequestMessage(timestamp = 0))
    recorder.checkPending()

    consistencyErrorMeter.count should be(before + 1)
    // TODO: verify service member status goes down

    verify(mockTxLog, atLeast(0)).commit() // Ignore all commit calls
    verifyZeroInteractions(mockTxLog)
  }

  test("append error should raise a consistency error") {
    val before = consistencyErrorMeter.count

    val request = createRequestMessage(timestamp = 0)
    recorder.handleMessage(request)
    recorder.checkPending()

    when(mockTxLog.append(anyObject())).thenThrow(new RuntimeException())
    recorder.handleMessage(createResponseMessage(request))
    recorder.advanceTime(appendDelay + 1000)
    recorder.checkPending()

    consistencyErrorMeter.count should be(before + 1)
    // TODO: verify service member status goes down

    verify(mockTxLog).append(argThat(matchTx(request)))
    verify(mockTxLog, atLeast(0)).commit() // Ignore all commit calls
    verifyZeroInteractions(mockTxLog)
  }

  test("transaction not responded in time should raise a consistency error") {
    val before = consistencyErrorMeter.count

    val request = createRequestMessage(timestamp = 0)
    recorder.handleMessage(request)
    recorder.checkPending()
    recorder.pendingSize should be(1)

    recorder.advanceTime(service.responseTimeout + 2000)
    recorder.checkPending()

    consistencyErrorMeter.count should be(before + 1)
    // TODO: verify service member status goes down

    verify(mockTxLog, atLeast(0)).commit() // Ignore all commit calls
    verifyZeroInteractions(mockTxLog)
  }
}
