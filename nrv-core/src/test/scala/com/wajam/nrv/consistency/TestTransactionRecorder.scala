package com.wajam.nrv.consistency

import com.wajam.nrv.service.{ActionSupportOptions, ServiceMember, Service}
import com.wajam.nrv.cluster.LocalNode

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.BeforeAndAfter
import org.mockito.Mockito._
import org.mockito.Matchers._
import com.wajam.nrv.data.InMessage
import org.scalatest.matchers.ShouldMatchers._

import com.wajam.nrv.utils.IdGenerator
import org.scalatest.mock.MockitoSugar
import com.wajam.nrv.utils.timestamp.Timestamp
import com.yammer.metrics.scala.Meter
import com.yammer.metrics.Metrics
import java.util.concurrent.TimeUnit
import persistence.LogRecord.Index
import persistence.{TransactionLogIterator, LogRecord, TransactionLog}


@RunWith(classOf[JUnitRunner])
class TestTransactionRecorder extends TestTransactionBase with BeforeAndAfter with MockitoSugar {

  var currentTime = 0L
  var consistencyErrorMeter: Meter = null
  val consistencyDelay = 1000L
  var service: Service = null
  var member: ServiceMember = null
  var fakeTxLog: FakeTransactionLog = null
  var recorder: TransactionRecorder = null

  before {
    service = new Service("service", new ActionSupportOptions(responseTimeout = Some(20000L)))
    member = new ServiceMember(0, new LocalNode(Map("nrv" -> 1234)))
    fakeTxLog = new FakeTransactionLog

    // Recorder currentTime and LogRecord id generation are mapped to the same variable.
    recorder = new TransactionRecorder(service, member, fakeTxLog, consistencyDelay, commitFrequency = 0,
      idGenerator = new IdGenerator[Long] {
        def nextId = TestTransactionRecorder.this.currentTime
      }) {
      override def currentTime = TestTransactionRecorder.this.currentTime
    }
    recorder.start()

    consistencyErrorMeter = new Meter(Metrics.defaultRegistry().newMeter(
      recorder.getClass, "consistency-error", "consistency-error", TimeUnit.MILLISECONDS))
  }

  after {
    consistencyErrorMeter = null
    recorder.kill()
    recorder = null
    fakeTxLog = null
    member = null
    service = null
  }

  trait TransactionAppender {
    def append(record: LogRecord)
  }

  class FakeTransactionLog extends TransactionLog {

    val mockAppender = mock[TransactionAppender]

    def getLastLoggedIndex = None

    def append[T <: LogRecord](block: => T): T = {
      val record: T = block
      mockAppender.append(record)
      record
    }

    def read(id: Option[Long], consistentTimestamp: Option[Timestamp]) = NullTransactionLogIterator

    def truncate(index: Index) {}

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
    verify(fakeTxLog.mockAppender).append(LogRecord(currentTime, None, request))
    recorder.checkPending()

    currentTime += 1
    val response = createResponseMessage(request)
    recorder.appendMessage(response)
    verify(fakeTxLog.mockAppender).append(LogRecord(currentTime, None, response))

    currentTime += consistencyDelay + 1
    recorder.checkPending()
    verify(fakeTxLog.mockAppender).append(Index(currentTime, Some(123)))
    verifyZeroInteractions(fakeTxLog.mockAppender)
  }

  test("success response without timestamp should raise a consistency error") {
    val before = consistencyErrorMeter.count

    recorder.pendingSize should be(0)
    recorder.appendMessage(createResponseMessage(new InMessage()))
    recorder.checkPending()
    recorder.pendingSize should be(0)

    consistencyErrorMeter.count should be(before + 1)
    // TODO: verify service member status goes down

    verifyZeroInteractions(fakeTxLog.mockAppender)
  }

  test("error response for non pending transaction should be ignored") {
    // two cases: 1) no timestamp in response and 2) no matching timestamp
    val before = consistencyErrorMeter.count

    // Error response without timestamp. Should not even try to append.
    recorder.appendMessage(createResponseMessage(new InMessage(), code = 500)) // no timestamp
    recorder.pendingSize should be(0)
    verifyZeroInteractions(fakeTxLog.mockAppender)

    // Error response with timestamp without pending match.
    currentTime += 1
    val response = createResponseMessage(createRequestMessage(timestamp = 0), code = 500)
    recorder.appendMessage(response)
    recorder.checkPending()
    recorder.pendingSize should be(0)
    verify(fakeTxLog.mockAppender).append(LogRecord(currentTime, None, response))

    consistencyErrorMeter.count should be(before)

    verifyZeroInteractions(fakeTxLog.mockAppender)
  }

  test("duplicate requests should raise a consistency error") {
    val before = consistencyErrorMeter.count

    val request = createRequestMessage(timestamp = 0)
    recorder.appendMessage(request)
    recorder.checkPending()
    recorder.pendingSize should be(1)
    verify(fakeTxLog.mockAppender).append(LogRecord(currentTime, None, request))

    // message with duplicate timestamp
    currentTime += 1
    recorder.appendMessage(request)
    recorder.checkPending()
    verify(fakeTxLog.mockAppender).append(LogRecord(currentTime, None, request))

    consistencyErrorMeter.count should be(before + 1)
    // TODO: verify service member status goes down

    verifyZeroInteractions(fakeTxLog.mockAppender)
  }

  test("request message append error should raise a consistency error") {
    val before = consistencyErrorMeter.count

    val request = createRequestMessage(timestamp = 0)

    when(fakeTxLog.mockAppender.append(anyObject())).thenThrow(new RuntimeException())
    evaluating {
      recorder.appendMessage(request)
    } should produce[ConsistencyException]
    recorder.checkPending()
    recorder.pendingSize should be(0)
    verify(fakeTxLog.mockAppender).append(LogRecord(currentTime, None, request))

    consistencyErrorMeter.count should be(before + 1)
    // TODO: verify service member status goes down
    verifyZeroInteractions(fakeTxLog.mockAppender)
  }

  test("response message append error should raise a consistency error") {
    val before = consistencyErrorMeter.count

    val request = createRequestMessage(timestamp = 123)
    recorder.appendMessage(request)
    recorder.checkPending()
    verify(fakeTxLog.mockAppender).append(LogRecord(currentTime, None, request))

    currentTime += 1
    val response = createResponseMessage(request)
    response.error should be(None)
    when(fakeTxLog.mockAppender.append(anyObject())).thenThrow(new RuntimeException())
    recorder.appendMessage(response)
    verify(fakeTxLog.mockAppender).append(LogRecord(currentTime, None, createResponseMessage(request)))
    response.error should not be (None)

    consistencyErrorMeter.count should be(before + 1)
    // TODO: verify service member status goes down
    verifyZeroInteractions(fakeTxLog.mockAppender)
  }

  test("transaction not responded in time should raise a consistency error") {
    val before = consistencyErrorMeter.count

    val response = createRequestMessage(timestamp = 0)
    recorder.appendMessage(response)
    recorder.checkPending()
    recorder.pendingSize should be(1)
    verify(fakeTxLog.mockAppender).append(LogRecord(currentTime, None, response))

    // Advance recorder time
    currentTime += recorder.consistencyTimeout + 1
    recorder.checkPending()

    consistencyErrorMeter.count should be(before + 1)
    // TODO: verify service member status goes down

    verifyZeroInteractions(fakeTxLog.mockAppender)
  }
}