package com.wajam.nrv.consistency.log

import com.wajam.nrv.consistency.TestTransactionBase
import com.wajam.nrv.consistency.log.LogRecord.{Response, Index}
import org.scalatest.Matchers._
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import com.wajam.nrv.service.TokenRange

@RunWith(classOf[JUnitRunner])
class TestTransactionLog extends TestTransactionBase with MockitoSugar {

  class MockTransactionLogIterator(records: LogRecord*) extends TransactionLogIterator {
    var closeCount = 0
    val itr = records.toIterator

    def hasNext = itr.hasNext

    def next() = itr.next()

    def close() {
      closeCount += 1
    }
  }

  test("first timestamped record should returns expected record") {
    val expectedRequest = LogRecord(id = 100, None, createRequestMessage(timestamp = 100))
    val iterator = new MockTransactionLogIterator(expectedRequest)

    val txLogProxy = new TransactionLogProxy
    when(txLogProxy.mockTxLog.read(100L)).thenReturn(iterator)

    iterator.closeCount should be(0)
    txLogProxy.firstRecord(Some(100L)) should be(Some(expectedRequest))
    iterator.closeCount should be(1)
  }

  test("first timestamped record should returns expected response record") {
    val expectedResponse = LogRecord(id = 100, None, createResponseMessage(createRequestMessage(timestamp = 100)))
    val iterator = new MockTransactionLogIterator(expectedResponse)

    val txLogProxy = new TransactionLogProxy
    when(txLogProxy.mockTxLog.read(100L)).thenReturn(iterator)

    iterator.closeCount should be(0)
    txLogProxy.firstRecord(Some(100L)) should be(Some(expectedResponse))
    iterator.closeCount should be(1)
  }

  test("first timestamped record called with None should returns expected record") {
    val expectedRequest = LogRecord(id = 100, None, createRequestMessage(timestamp = 100))
    val iterator = new MockTransactionLogIterator(expectedRequest)

    val txLogProxy = new TransactionLogProxy
    when(txLogProxy.mockTxLog.read(Index(Long.MinValue))).thenReturn(iterator)

    iterator.closeCount should be(0)
    txLogProxy.firstRecord(None) should be(Some(expectedRequest))
    iterator.closeCount should be(1)
  }

  test("first timestamped record should skip index") {
    val expectedRequest = LogRecord(id = 100, None, createRequestMessage(timestamp = 100))
    val iterator = new MockTransactionLogIterator(Index(0L), expectedRequest)

    val txLogProxy = new TransactionLogProxy
    when(txLogProxy.mockTxLog.read(100L)).thenReturn(iterator)

    iterator.closeCount should be(0)
    txLogProxy.firstRecord(Some(100L)) should be(Some(expectedRequest))
    iterator.closeCount should be(1)
  }

  test("first timestamped record should not fail when log is empty") {
    val iterator = new MockTransactionLogIterator()

    val txLogProxy = new TransactionLogProxy
    when(txLogProxy.mockTxLog.read(100L)).thenReturn(iterator)

    iterator.closeCount should be(0)
    txLogProxy.firstRecord(Some(100L)) should be(None)
    iterator.closeCount should be(1)
  }

  test("first timestamped record should not fail when log only contains index") {
    val iterator = new MockTransactionLogIterator(Index(0L), Index(1L))

    val txLogProxy = new TransactionLogProxy
    when(txLogProxy.mockTxLog.read(100L)).thenReturn(iterator)

    iterator.closeCount should be(0)
    txLogProxy.firstRecord(Some(100L)) should be(None)
    iterator.closeCount should be(1)
  }

  test("last successful ts should returns expected timestamp") {
    val response1 = Response(100, None, 100L, token = 100, Response.Success)
    val response2 = Response(200, None, 200L, token = 200, Response.Success)
    val response3 = Response(300, None, 300L, token = 300, Response.Success)
    val iterator = new MockTransactionLogIterator(response2, response3, response1)

    val txLogProxy = new TransactionLogProxy
    when(txLogProxy.mockTxLog.read(100L)).thenReturn(iterator)

    iterator.closeCount should be(0)
    txLogProxy.lastSuccessfulTimestamp(100L) should be(Some(response3.timestamp))
    iterator.closeCount should be(1)
  }

  test("last successful ts should returns expected timestamp in specified token ranges") {
    val response1 = Response(100, None, 100L, token = 100, Response.Success)
    val response2 = Response(200, None, 200L, token = 200, Response.Success)
    val response3 = Response(300, None, 300L, token = 300, Response.Success)
    val iterator = new MockTransactionLogIterator(response2, response3, response1)

    val txLogProxy = new TransactionLogProxy
    when(txLogProxy.mockTxLog.read(100L)).thenReturn(iterator)

    iterator.closeCount should be(0)
    txLogProxy.lastSuccessfulTimestamp(100L, Seq(TokenRange(0, 200))) should be(Some(response2.timestamp))
    iterator.closeCount should be(1)
  }

  test("last successful ts should ignore error response") {
    val response1 = Response(100, None, 100L, token = 100, Response.Success)
    val response2 = Response(200, None, 200L, token = 200, Response.Success)
    val response3 = Response(300, None, 300L, token = 300, Response.Error)
    val iterator = new MockTransactionLogIterator(response2, response3, response1)

    val txLogProxy = new TransactionLogProxy
    when(txLogProxy.mockTxLog.read(100L)).thenReturn(iterator)

    iterator.closeCount should be(0)
    txLogProxy.lastSuccessfulTimestamp(100L) should be(Some(response2.timestamp))
    iterator.closeCount should be(1)
  }

  test("last successful ts should not fail if log is empty") {
    val iterator = new MockTransactionLogIterator()

    val txLogProxy = new TransactionLogProxy
    when(txLogProxy.mockTxLog.read(100L)).thenReturn(iterator)

    iterator.closeCount should be(0)
    txLogProxy.lastSuccessfulTimestamp(100L) should be(None)
    iterator.closeCount should be(1)
  }

  test("last successful ts should not fail if log does not contains success response") {
    val request = LogRecord(id = 100, None, createRequestMessage(timestamp = 100))
    val response = Response(100, None, 100L, 0, Response.Error)
    val index = Index(300, Some(100L))
    val iterator = new MockTransactionLogIterator(request, response, index)

    val txLogProxy = new TransactionLogProxy
    when(txLogProxy.mockTxLog.read(100L)).thenReturn(iterator)

    iterator.closeCount should be(0)
    txLogProxy.lastSuccessfulTimestamp(100L) should be(None)
    iterator.closeCount should be(1)
  }

  test("last successful ts should not fail if log does not contains success response for the specified range") {
    val response1 = Response(100, None, 100L, token = 100, Response.Error)
    val response2 = Response(200, None, 200L, token = 200, Response.Success)
    val index = Index(300, Some(200L))
    val iterator = new MockTransactionLogIterator(response1, response2, index)

    val txLogProxy = new TransactionLogProxy
    when(txLogProxy.mockTxLog.read(100L)).thenReturn(iterator)

    iterator.closeCount should be(0)
    txLogProxy.lastSuccessfulTimestamp(100L, Seq(TokenRange(0, 100))) should be(None)
    iterator.closeCount should be(1)
  }
}
