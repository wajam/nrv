package com.wajam.nrv.consistency

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.BeforeAndAfter
import java.io.File
import java.nio.file.Files
import org.mockito.Mockito._
import persistence.LogRecord.{Response, Request, Index}
import persistence.{LogRecord, FileTransactionLog, LogRecordSerializer}
import com.wajam.nrv.service.TokenRange
import org.scalatest.mock.MockitoSugar
import com.wajam.nrv.utils.{ControlableCurrentTime, IdGenerator, TimestampIdGenerator}
import org.scalatest.matchers.ShouldMatchers._
import com.wajam.nrv.data.InMessage


@RunWith(classOf[JUnitRunner])
class TestTransactionRecovery extends TestTransactionBase with BeforeAndAfter with MockitoSugar {

  var member: ResolvedServiceMember = null
  var logDir: File = null
  var mockStore: ConsistentStore = null
  var idGenerator: SequenceIdGenerator = null
//  var recoveryDir: File = null

  before {
    logDir = Files.createTempDirectory("TestFileTransactionLog").toFile
//    recoveryDir = new File(logDir, "recovery")
    member = ResolvedServiceMember(serviceName = "service", token = 1000L, ranges = Seq(TokenRange.All))
//    fileTxLog = createFileTransactionLog()
    mockStore = mock[ConsistentStore]
    idGenerator = new SequenceIdGenerator
  }

  after {
//    fileTxLog.close()
//    fileTxLog = null
    idGenerator = null
    mockStore = null
    member = null

    logDir.listFiles().foreach(_.delete())
    logDir.delete()
    logDir = null
  }

  class SequenceIdGenerator(seed: Long = 0L) extends IdGenerator[Long] {
    var lastId = seed

    def nextId = {
      lastId += 1
      lastId
    }
  }
  def createTxLog(member: ResolvedServiceMember = member, dir: String = logDir.getAbsolutePath,
                               fileRolloverSize: Int = 0) = {
    new FileTransactionLog(member.serviceName, member.token, dir, fileRolloverSize = fileRolloverSize)
  }

  test("empty log directory should not fail") {
    val recovery = new TransactionRecovery(logDir.getAbsolutePath, mockStore, idGenerator)

    logDir.list() should be(Array())
    recovery.recoverMember(member)
    logDir.list() should be(Array())
    verifyZeroInteractions(mockStore)
  }

  test("comfirmed complete transaction should NOT BE rewritten") {
    val txLog = createTxLog()
    val request = createRequestMessage(timestamp = 0)
    val r1 = txLog.append(Request(1, None, request))
    val r2 = txLog.append(Response(2, None, createResponseMessage(request)))
    val r3 = txLog.append(Index(3, consistentTimestamp = Some(r1.timestamp)))
    txLog.commit()

    val recovery = new TransactionRecovery(logDir.getAbsolutePath, mockStore, idGenerator)
    recovery.recoverMember(member)

    verifyZeroInteractions(mockStore)

    txLog.read().toList should be(List(r1, r2, r3))

    logDir.list() should be(Array("service-0000001000-1:.log"))
  }

  test("uncomfirmed complete transaction should BE rewritten") {
    val txLog = createTxLog()
    val request = createRequestMessage(timestamp = 0)
    val r1 = txLog.append(Request(1, None, request))
    val r2 = txLog.append(Response(2, None, createResponseMessage(request)))
    txLog.commit()

    idGenerator.lastId = 300
    val recovery = new TransactionRecovery(logDir.getAbsolutePath, mockStore, idGenerator)
    recovery.recoverMember(member)

    verifyZeroInteractions(mockStore)

    val r301 = Request(301, None, request)
    val r302 = Response(302, None, createResponseMessage(request))
    val r303 = Index(303, Some(r1.timestamp))
    txLog.read().toList should be(List(r301, r302, r303))

    logDir.list() should be(Array("service-0000001000-1:.log", "service-0000001000-301:.log"))
  }

  test("incomplete transaction should BE truncated") {
    val txLog = createTxLog()
    val request1 = createRequestMessage(timestamp = 11, token = 0) // complete and confirmed by request3 response, not rewritten
    val request2 = createRequestMessage(timestamp = 12, token = 1) // incomplete, truncated
    val request3 = createRequestMessage(timestamp = 13, token = 2) // complete but unconfirmed, rewritten
    val request4 = createRequestMessage(timestamp = 14, token = 0) // complete but unconfirmed, rewritten
    val r1 = txLog.append(Request(1, None, request3))
    val r2 = txLog.append(Request(2, None, request1))
    val r3 = txLog.append(Request(3, None, request2))
    val r4 = txLog.append(Response(4, None, createResponseMessage(request1)))
    val r5 = txLog.append(Request(5, None, request4))
    val r6 = txLog.append(Response(6, Some(r1.timestamp), createResponseMessage(request4)))
    val r7 = txLog.append(Response(7, Some(r1.timestamp), createResponseMessage(request3)))
    txLog.commit()

    idGenerator.lastId = 300
    val recovery = new TransactionRecovery(logDir.getAbsolutePath, mockStore, idGenerator)
    recovery.recoverMember(member)

    verify(mockStore).truncateAt(Consistency.getMessageTimestamp(request2).get, request2.token)

    val r301 = Request(301, None, request1)
    val r302 = Response(302, Some(r1.timestamp), createResponseMessage(request3))
    val r303 = Request(303, Some(r302.timestamp), request4)
    val r304 = Response(304, Some(r302.timestamp), createResponseMessage(request4))
    txLog.read().toList should be(List(r1, r301, r302, r303, r304))

    logDir.list() should be(Array("service-0000001000-1:.log", "service-0000001000-301:.log"))
  }
}
