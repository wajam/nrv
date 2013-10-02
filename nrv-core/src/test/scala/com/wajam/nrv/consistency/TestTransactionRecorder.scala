package com.wajam.nrv.consistency

import com.wajam.nrv.service.{TokenRange, ServiceMember, Service}
import com.wajam.nrv.cluster.LocalNode

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.BeforeAndAfter
import org.mockito.Mockito._
import org.mockito.Matchers._
import com.wajam.nrv.data.{Message, InMessage}
import org.scalatest.matchers.ShouldMatchers._

import org.scalatest.mock.MockitoSugar
import com.wajam.nrv.consistency.log.LogRecord.Index
import com.wajam.nrv.consistency.log.{TransactionLogProxy, LogRecord}
import com.wajam.commons.IdGenerator
import com.wajam.nrv.utils.timestamp.Timestamp

@RunWith(classOf[JUnitRunner])
class TestTransactionRecorder extends TestTransactionBase with BeforeAndAfter with MockitoSugar {

  var consistencyErrorCount = 0
  var currentTime = 0L
  val consistencyDelay = 1000L
  val consistencyTimeout = 15000L
  var service: Service = null
  var member: ServiceMember = null
  var txLogProxy: TransactionLogProxy = null
  var recorder: TransactionRecorder = null

  before {
    service = new Service("service")
    member = new ServiceMember(0, new LocalNode(Map("nrv" -> 1234)))
    txLogProxy = new TransactionLogProxy

    // Recorder currentTime and LogRecord id generation are mapped to the same variable.
    recorder = new TransactionRecorder(ResolvedServiceMember(service.name, member.token, Seq(TokenRange.All)),
      txLogProxy, consistencyDelay, consistencyTimeout, commitFrequency = 0,
      onConsistencyError = consistencyErrorCount += 1,
      idGenerator = new IdGenerator[Long] {
        def nextId = TestTransactionRecorder.this.currentTime
      }) {
      override def currentTime = TestTransactionRecorder.this.currentTime
    }
    recorder.start()
  }

  after {
    consistencyErrorCount = 0
    recorder.kill()
    recorder = null
    txLogProxy = null
    member = null
    service = null
  }

  test("tx consistency should be pending until responded") {
    recorder.pendingSize should be(0)
    val request = createRequestMessage(timestamp = 0)
    recorder.appendMessage(request)
    recorder.checkPending()
    recorder.pendingSize should be(1)

    val response = createResponseMessage(request)
    currentTime += 1
    recorder.appendMessage(response)
    currentTime += consistencyDelay
    recorder.checkPending()
    recorder.pendingSize should be(0)
    consistencyErrorCount should be(0)
  }

  test("messages should be appended to log immediatly") {
    val request = createRequestMessage(timestamp = 123)
    recorder.appendMessage(request)
    verify(txLogProxy.mockAppender).append(LogRecord(currentTime, None, request))
    recorder.checkPending()

    currentTime += 1
    val response = createResponseMessage(request)
    recorder.appendMessage(response)
    verify(txLogProxy.mockAppender).append(LogRecord(currentTime, None, response))

    currentTime += consistencyDelay + 1
    recorder.checkPending()

    consistencyErrorCount should be(0)
    verify(txLogProxy.mockAppender).append(Index(currentTime, Some(123)))
    verifyZeroInteractions(txLogProxy.mockAppender)
    txLogProxy.verifyZeroInteractions()
  }

  test("success response without timestamp should raise a consistency error") {
    recorder.pendingSize should be(0)
    recorder.appendMessage(createResponseMessage(new InMessage()))
    recorder.checkPending()

    recorder.pendingSize should be(0)
    consistencyErrorCount should be(1)
    txLogProxy.verifyZeroInteractions()
  }

  test("error response for non pending transaction should be ignored") {
    // two cases: 1) no timestamp in response and 2) no matching timestamp

    // Error response without timestamp. Should not even try to append.
    recorder.appendMessage(createResponseMessage(new InMessage(), code = 500)) // no timestamp
    recorder.pendingSize should be(0)
    verifyZeroInteractions(txLogProxy.mockAppender)

    // Error response with timestamp without pending match.
    currentTime += 1
    val response = createResponseMessage(createRequestMessage(timestamp = 0), code = 500)
    recorder.appendMessage(response)
    recorder.checkPending()

    recorder.pendingSize should be(0)
    consistencyErrorCount should be(0)
    verify(txLogProxy.mockAppender).append(LogRecord(currentTime, None, response))
    txLogProxy.verifyZeroInteractions()
  }

  test("duplicate requests should raise a consistency error") {
    val request = createRequestMessage(timestamp = 0)
    recorder.appendMessage(request)
    recorder.checkPending()
    recorder.pendingSize should be(1)
    verify(txLogProxy.mockAppender).append(LogRecord(currentTime, None, request))

    // message with duplicate timestamp
    currentTime += 1
    recorder.appendMessage(request)
    recorder.checkPending()

    consistencyErrorCount should be(1)
    verify(txLogProxy.mockAppender).append(LogRecord(currentTime, None, request))
    txLogProxy.verifyZeroInteractions()
  }

  test("request message append error should raise a consistency error") {
    val request = createRequestMessage(timestamp = 0)

    when(txLogProxy.mockAppender.append(anyObject())).thenThrow(new RuntimeException())
    evaluating {
      recorder.appendMessage(request)
    } should produce[ConsistencyException]
    recorder.checkPending()

    consistencyErrorCount should be(1)
    recorder.pendingSize should be(0)
    verify(txLogProxy.mockAppender).append(LogRecord(currentTime, None, request))
    txLogProxy.verifyZeroInteractions()
  }

  test("response message append error should raise a consistency error") {
    val request = createRequestMessage(timestamp = 123)
    recorder.appendMessage(request)
    recorder.checkPending()
    verify(txLogProxy.mockAppender).append(LogRecord(currentTime, None, request))

    currentTime += 1
    val response = createResponseMessage(request)
    response.error should be(None)
    when(txLogProxy.mockAppender.append(anyObject())).thenThrow(new RuntimeException())
    evaluating {
      recorder.appendMessage(response)
    } should produce[ConsistencyException]

    consistencyErrorCount should be(1)
    verify(txLogProxy.mockAppender).append(LogRecord(currentTime, None, createResponseMessage(request)))
    response.error should be(None)
    txLogProxy.verifyZeroInteractions()
  }

  test("transaction not responded in time should raise a consistency error") {
    val response = createRequestMessage(timestamp = 0)
    recorder.appendMessage(response)
    recorder.checkPending()
    recorder.pendingSize should be(1)
    verify(txLogProxy.mockAppender).append(LogRecord(currentTime, None, response))

    // Advance recorder time
    currentTime += consistencyTimeout + 1
    recorder.checkPending()
    recorder.checkPending() // Call again to ensure the same tx is not timing out again

    consistencyErrorCount should be(1)
    txLogProxy.verifyZeroInteractions()
  }

  test("consistent timestamp should not have a transaction with a greater timestamp appended before") {
    val request1 = createRequestMessage(timestamp = 1)
    val request2 = createRequestMessage(timestamp = 2)
    val request3 = createRequestMessage(timestamp = 3)
    val request4 = createRequestMessage(timestamp = 4)
    val request5 = createRequestMessage(timestamp = 5)
    val request6 = createRequestMessage(timestamp = 6)
    val request7 = createRequestMessage(timestamp = 7)
    val request8 = createRequestMessage(timestamp = 8)
    val request9 = createRequestMessage(timestamp = 9)

    // Test assume consistency delay is 1 sec
    consistencyDelay should be(1000)

    def appendMessage(id: Long, message: Message) {
      currentTime = id
      recorder.checkPending()
      recorder.appendMessage(message)
      recorder.checkPending()
    }

    appendMessage(10, request1)
    appendMessage(20, request3)
    appendMessage(30, request4)
    appendMessage(40, createResponseMessage(request4))
    appendMessage(50, createResponseMessage(request1))
    appendMessage(60, request2)
    appendMessage(1055, request5)
    appendMessage(1060, createResponseMessage(request3))
    appendMessage(1070, createResponseMessage(request2))
    appendMessage(1080, createResponseMessage(request5))
    appendMessage(1090, request6)
    appendMessage(2000, request7)
    appendMessage(3000, createResponseMessage(request6))
    appendMessage(3010, createResponseMessage(request7))
    appendMessage(4000, request8)
    appendMessage(4010, request9)
    appendMessage(4020, createResponseMessage(request9))
    appendMessage(4030, createResponseMessage(request8))
    currentTime = 5500
    recorder.checkPending()

    verify(txLogProxy.mockAppender).append(LogRecord(10, None, request1))
    verify(txLogProxy.mockAppender).append(LogRecord(20, None, request3))
    verify(txLogProxy.mockAppender).append(LogRecord(30, None, request4))
    verify(txLogProxy.mockAppender).append(LogRecord(40, None, createResponseMessage(request4)))
    verify(txLogProxy.mockAppender).append(LogRecord(50, None, createResponseMessage(request1)))
    verify(txLogProxy.mockAppender).append(LogRecord(60, None, request2))
    verify(txLogProxy.mockAppender).append(LogRecord(1055, Some(1), request5))
    verify(txLogProxy.mockAppender).append(LogRecord(1060, Some(1), createResponseMessage(request3)))
    verify(txLogProxy.mockAppender).append(LogRecord(1070, Some(1), createResponseMessage(request2)))
    verify(txLogProxy.mockAppender).append(LogRecord(1080, Some(4), createResponseMessage(request5)))
    verify(txLogProxy.mockAppender).append(LogRecord(1090, Some(4), request6))
    verify(txLogProxy.mockAppender).append(LogRecord(2000, Some(4), request7))
    verify(txLogProxy.mockAppender).append(LogRecord(3000, Some(5), createResponseMessage(request6)))
    verify(txLogProxy.mockAppender).append(LogRecord(3010, Some(6), createResponseMessage(request7)))
    verify(txLogProxy.mockAppender).append(Index(3010, Some(7))) // id is a dupe but this is just a test...
    verify(txLogProxy.mockAppender).append(LogRecord(4000, Some(7), request8))
    verify(txLogProxy.mockAppender).append(LogRecord(4010, Some(7), request9))
    verify(txLogProxy.mockAppender).append(LogRecord(4020, Some(7), createResponseMessage(request9)))
    verify(txLogProxy.mockAppender).append(LogRecord(4030, Some(7), createResponseMessage(request8)))
    verify(txLogProxy.mockAppender).append(Index(5500, Some(9)))

    currentTime = 10000
    recorder.checkPending()
    recorder.currentConsistentTimestamp should be(Some(Timestamp(9)))

    txLogProxy.verifyNoMoreInteractions()
  }
}
