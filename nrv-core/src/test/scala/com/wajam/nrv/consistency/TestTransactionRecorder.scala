package com.wajam.nrv.consistency

import com.wajam.nrv.service.{ActionSupportOptions, ServiceMember, Service}
import com.wajam.nrv.cluster.LocalNode

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.BeforeAndAfter
import org.mockito.Mockito._
import org.mockito.Matchers._
import com.wajam.nrv.data.{Message, MessageType, InMessage, OutMessage}
import org.mockito.{ArgumentCaptor, ArgumentMatcher}
import org.hamcrest.Description
import org.scalatest.matchers.ShouldMatchers._

import com.wajam.nrv.utils.{IdGenerator, ControlableCurrentTime}
import org.scalatest.mock.MockitoSugar
import com.wajam.nrv.utils.timestamp.Timestamp
import com.yammer.metrics.scala.Meter
import com.yammer.metrics.Metrics
import java.util.concurrent.TimeUnit
import persistence.LogRecord.{Index, Response, Request}
import persistence.{TransactionLogIterator, LogRecord, TransactionLog}


@RunWith(classOf[JUnitRunner])
class TestTransactionRecorder extends TestTransactionBase with BeforeAndAfter with MockitoSugar {

  var currentTime = 0L
  var consistencyErrorMeter: Meter = null
  val consistencyDelay = 1000L
  var service: Service = null
  var member: ServiceMember = null
  var mockTxLog: MockTransactionLog = null
  var recorder: TransactionRecorder /*with ControlableCurrentTime*/ = null

  before {
    service = new Service("service", new ActionSupportOptions(responseTimeout = Some(20000L)))
    member = new ServiceMember(0, new LocalNode(Map("nrv" -> 1234)))
    mockTxLog = new MockTransactionLog

    // Recorder currentTime and LogRecord id generation are mapped to the same variable.
    recorder = new TransactionRecorder(service, member, mockTxLog, consistencyDelay, new IdGenerator[Long] {
      def nextId = TestTransactionRecorder.this.currentTime
    }) {
      override def currentTime = TestTransactionRecorder.this.currentTime
    }
    recorder.start()
//    verify(mockTxLog).getLastLoggedIndex // Ignore call done at recorder construction

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

//  private class LogRecordMatcher(message: Message) extends ArgumentMatcher {
//    val timestamp = Consistency.getMessageTimestamp(message).get
//    val token = message.token
//
//    def matches(argument: Any): Boolean = {
//      argument match {
//        case record: Request => record == Request(timestamp.value, None)
//        case record: Response =>
//        case record: Index =>
//      }
//      val record = argument.asInstanceOf[LogRecord]
//      record. == token &&
//        tx.message.parameters == message.parameters && tx.message.metadata == message.metadata &&
//        tx.message.messageData == message.messageData
//    }
//
//    override def describeTo(description: Description) {
//      description.appendValue(TransactionEvent(message, previous))
//    }
//  }
//
//  private def matchTx(message: Message, previous: Option[Timestamp] = None) = {
//    new TransactionEventMatcher(message, previous)
//  }

//  private def createRequestMessage(timestamp: Long, token: Long = 0): InMessage = {
//    val request = new InMessage((Map(("ts" -> timestamp), ("tk" -> token))))
//    request.function = MessageType.FUNCTION_CALL
//    request.token = token
//    request.serviceName = service.name
//    Consistency.setMessageTimestamp(request, Timestamp(timestamp))
//    request
//  }
//
//  private def createResponseMessage(request: InMessage, code: Int = 200, error: Option[Exception] = None) = {
//    val response = new OutMessage()
//    request.copyTo(response)
//    response.function = MessageType.FUNCTION_RESPONSE
//    response.code = code
//    response.error = error
//    response
//  }


  trait TransactionAppender {
    def append(record: LogRecord)
  }

  class MockTransactionLog extends TransactionLog {

    val mockAppender = mock[TransactionAppender]

    def getLastLoggedIndex = None

    def append[T <: LogRecord](block: => T): T = {
      val record: T = block
      mockAppender.append(record)
      record
    }

    def read(id: Option[Long], consistentTimestamp: Option[Timestamp]) = NullTransactionLogIterator

    def truncate(index: Index) = true

    def commit() {}

    def close() {}

    object NullTransactionLogIterator extends TransactionLogIterator {
      def hasNext = false

      def next() = null

      def close() {}
    }

  }
  test("tx consistency should be pending until responded") {
    recorder.pendingSize should be(0)
    val request = createRequestMessage(timestamp = 0)
    recorder.appendMessage(request)
    recorder.checkPending()
    recorder.pendingSize should be(1)

    val response = createResponseMessage(request)
    recorder.appendMessage(response)
    currentTime += consistencyDelay + 1
    recorder.checkPending()
    recorder.pendingSize should be(0)
  }

  test("messages should be appended to log immediatly") {
    val request = createRequestMessage(timestamp = 123)
    recorder.appendMessage(request)
    verify(mockTxLog.mockAppender).append(LogRecord(currentTime, None, request))
    recorder.checkPending()

    currentTime += 1
    val response = createResponseMessage(request)
    recorder.appendMessage(response)
    verify(mockTxLog.mockAppender).append(LogRecord(currentTime, None, response))

    currentTime += consistencyDelay + 1
    recorder.checkPending()
    verify(mockTxLog.mockAppender).append(Index(currentTime, Some(123)))
    verifyZeroInteractions(mockTxLog.mockAppender)
  }

//  test("successful transaction should NOT be appended to log BEFORE append delay") {
//    val request = createRequestMessage(timestamp = 0)
//    recorder.handleMessage(request)
//    recorder.checkPending()
//    recorder.pendingSize should be(1)
//
//    recorder.handleMessage(createResponseMessage(request))
//    recorder.checkPending()
//    recorder.pendingSize should be(1)
//
//    verify(mockTxLog, atLeast(0)).commit() // Ignore all commit calls
//    verifyZeroInteractions(mockTxLog)
//  }
//
//  test("successful transactions should be logged in timestamp order even if seen and responded out of order") {
//    val request1 = createRequestMessage(timestamp = 1)
//    val request2 = createRequestMessage(timestamp = 2)
//    val request3 = createRequestMessage(timestamp = 3)
//    val request4 = createRequestMessage(timestamp = 4)
//
//    recorder.handleMessage(request3)
//    recorder.handleMessage(request4)
//    recorder.handleMessage(request1)
//    recorder.handleMessage(request2)
//    recorder.checkPending() // Ensure requests are processed by actor
//
//    recorder.handleMessage(createResponseMessage(request3))
//    recorder.handleMessage(createResponseMessage(request4))
//    recorder.handleMessage(createResponseMessage(request2))
//    recorder.advanceTime(consistencyDelay + 1000)
//    recorder.checkPending()
//    recorder.handleMessage(createResponseMessage(request1))
//    recorder.checkPending()
//
//    verify(mockTxLog).append(argThat(matchTx(request1)))
//    verify(mockTxLog).append(argThat(matchTx(request2, Some(Timestamp(1)))))
//    verify(mockTxLog).append(argThat(matchTx(request3, Some(Timestamp(2)))))
//    verify(mockTxLog).append(argThat(matchTx(request4, Some(Timestamp(3)))))
//    verify(mockTxLog, atLeast(0)).commit() // Ignore all commit calls
//    verifyZeroInteractions(mockTxLog)
//  }
//
//  test("failed transaction (code) should not be appended to log") {
//    // not logged + not pending anymore
//    val request = createRequestMessage(timestamp = 0)
//    recorder.handleMessage(request)
//    recorder.checkPending()
//    recorder.pendingSize should be(1)
//
//    recorder.handleMessage(createResponseMessage(request, code = 500))
//    recorder.checkPending()
//    recorder.pendingSize should be(0)
//
//    verify(mockTxLog, atLeast(0)).commit() // Ignore all commit calls
//    verifyZeroInteractions(mockTxLog)
//  }
//
//  test("failed transaction (exception) should not be appended to log") {
//    // not logged + not pending anymore
//    val request = createRequestMessage(timestamp = 0)
//    recorder.handleMessage(request)
//    recorder.checkPending()
//    recorder.pendingSize should be(1)
//
//    recorder.handleMessage(createResponseMessage(request, error = Some(new Exception)))
//    recorder.checkPending()
//    recorder.pendingSize should be(0)
//
//    verify(mockTxLog, atLeast(0)).commit() // Ignore all commit calls
//    verifyZeroInteractions(mockTxLog)
//  }
//
//  ignore("timeout transaction should be truncated from storage") {
//  }
//
//  ignore("success response for non pending transaction should be truncated from storage") {
//  }
//
  test("success response without timestamp should raise a consistency error") {
    val before = consistencyErrorMeter.count

    recorder.pendingSize should be(0)
    recorder.appendMessage(createResponseMessage(new InMessage()))
    recorder.checkPending()
    recorder.pendingSize should be(0)

    consistencyErrorMeter.count should be(before + 1)
    // TODO: verify service member status goes down

    verifyZeroInteractions(mockTxLog.mockAppender)
  }

  test("error response for non pending transaction should be ignored") {
    // two cases: 1) no timestamp in response and 2) no matching timestamp
    val before = consistencyErrorMeter.count

    // Error response without timestamp. Should not even try to append.
    recorder.appendMessage(createResponseMessage(new InMessage(), code = 500)) // no timestamp
    recorder.pendingSize should be(0)
    verifyZeroInteractions(mockTxLog.mockAppender)

    // Error response with timestamp without pending match.
    currentTime += 1
    val response = createResponseMessage(createRequestMessage(timestamp = 0), code = 500)
    recorder.appendMessage(response)
    recorder.checkPending()
    recorder.pendingSize should be(0)
    verify(mockTxLog.mockAppender).append(LogRecord(currentTime, None, response))

    consistencyErrorMeter.count should be(before)

    verifyZeroInteractions(mockTxLog.mockAppender)
  }

  test("duplicate requests should raise a consistency error") {
    val before = consistencyErrorMeter.count

    val request = createRequestMessage(timestamp = 0)
    recorder.appendMessage(request)
    recorder.checkPending()
    recorder.pendingSize should be(1)
    verify(mockTxLog.mockAppender).append(LogRecord(currentTime, None, request))

    // message with duplicate timestamp
    currentTime += 1
    recorder.appendMessage(request)
    recorder.checkPending()
    verify(mockTxLog.mockAppender).append(LogRecord(currentTime, None, request))

    consistencyErrorMeter.count should be(before + 1)
    // TODO: verify service member status goes down

    verifyZeroInteractions(mockTxLog.mockAppender)
  }

//  test("append error should raise a consistency error") {
//    val before = consistencyErrorMeter.count
//
//    val request = createRequestMessage(timestamp = 0)
//    recorder.handleMessage(request)
//    recorder.checkPending()
//
//    when(mockTxLog.append(anyObject())).thenThrow(new RuntimeException())
//    recorder.handleMessage(createResponseMessage(request))
//    recorder.advanceTime(consistencyDelay + 1000)
//    recorder.checkPending()
//
//    consistencyErrorMeter.count should be(before + 1)
//    // TODO: verify service member status goes down
//
//    verify(mockTxLog).append(argThat(matchTx(request)))
//    verify(mockTxLog, atLeast(0)).commit() // Ignore all commit calls
//    verifyZeroInteractions(mockTxLog)
//  }
//
  test("transaction not responded in time should raise a consistency error") {
    val before = consistencyErrorMeter.count

    val request = createRequestMessage(timestamp = 0)
    recorder.appendMessage(request)
    recorder.checkPending()
    recorder.pendingSize should be(1)
    verify(mockTxLog.mockAppender).append(LogRecord(currentTime, None, request))

    // Advance recorder time
    currentTime += recorder.consistencyTimeout + 1
    recorder.checkPending()

    consistencyErrorMeter.count should be(before + 1)
    // TODO: verify service member status goes down

    verifyZeroInteractions(mockTxLog.mockAppender)
  }
}