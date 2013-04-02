package com.wajam.nrv.consistency

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.BeforeAndAfter
import java.io.File
import java.nio.file.Files
import org.mockito.Mockito._
import persistence.LogRecord.{Response, Request, Index}
import persistence.{LogRecord, FileTransactionLog}
import com.wajam.nrv.service.TokenRange
import org.scalatest.mock.MockitoSugar
import com.wajam.nrv.utils.IdGenerator
import org.scalatest.matchers.ShouldMatchers._
import Consistency._


@RunWith(classOf[JUnitRunner])
class TestConsistencyRecovery extends TestTransactionBase with BeforeAndAfter with MockitoSugar {

  var member: ResolvedServiceMember = null
  var logDir: File = null
  var mockStore: ConsistentStore = null
  var idGenerator: SequenceIdGenerator = null

  before {
    logDir = Files.createTempDirectory("TestFileTransactionLog").toFile
    member = ResolvedServiceMember(serviceName = "service", token = 1000L, ranges = Seq(TokenRange.All))
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
    val recovery = new ConsistencyRecovery(logDir.getAbsolutePath, mockStore, idGenerator)

    logDir.list() should be(Array())
    val lastIndex = recovery.restoreMemberConsistency(member, onRecoveryFailure = fail())
    lastIndex should be(None)
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

    val recovery = new ConsistencyRecovery(logDir.getAbsolutePath, mockStore, idGenerator)
    val lastIndex = recovery.restoreMemberConsistency(member, onRecoveryFailure = fail())
    lastIndex should be(Some(r3))

    verifyZeroInteractions(mockStore)

    txLog.read.toList should be(List(r1, r2, r3))

    logDir.list() should be(Array("service-0000001000-1:.log"))
  }

  test("uncomfirmed complete transaction should BE rewritten") {
    val txLog = createTxLog()
    val request = createRequestMessage(timestamp = 0)
    val r1 = txLog.append(Request(1, None, request))
    val r2 = txLog.append(Response(2, None, createResponseMessage(request)))
    txLog.commit()

    idGenerator.lastId = 300
    val recovery = new ConsistencyRecovery(logDir.getAbsolutePath, mockStore, idGenerator)
    val lastIndex = recovery.restoreMemberConsistency(member, onRecoveryFailure = fail())

    verifyZeroInteractions(mockStore)

    val r301 = Request(301, None, request)
    val r302 = Response(302, None, createResponseMessage(request))
    val r303 = Index(303, getMessageTimestamp(request))
    txLog.read.toList should be(List(r301, r302, r303))

    logDir.list().toSet should be(Set("service-0000001000-1:.log", "service-0000001000-301:.log"))
  }

  test("incomplete transaction should BE truncated and complete transactions should BE rewritten") {
    val txLog = createTxLog()
    val request1 = createRequestMessage(timestamp = 1, token = 0) // complete
    val request2 = createRequestMessage(timestamp = 2, token = 1) // complete and confirmed by request3
    val request3 = createRequestMessage(timestamp = 3, token = 2) // complete but unconfirmed, rewritten
    val request4 = createRequestMessage(timestamp = 4, token = 0) // complete but unconfirmed, rewritten
    val request5 = createRequestMessage(timestamp = 5, token = 0) // incomplete, truncated
    val r1 = txLog.append(Request(1, None, request3))
    val r2 = txLog.append(Request(2, None, request2))
    val r3 = txLog.append(Request(3, None, request1))
    val r4 = txLog.append(Response(4, None, createResponseMessage(request2)))
    val r5 = txLog.append(Response(5, None, createResponseMessage(request1)))
    val r6 = txLog.append(Response(6, getMessageTimestamp(request2), createResponseMessage(request3)))
    val r7 = txLog.append(Request(7, getMessageTimestamp(request2), request4))
    val r8 = txLog.append(Request(8, getMessageTimestamp(request2), request5))
    val r9 = txLog.append(Response(9, getMessageTimestamp(request2), createResponseMessage(request4)))
    txLog.commit()

    idGenerator.lastId = 300
    val recovery = new ConsistencyRecovery(logDir.getAbsolutePath, mockStore, idGenerator)
    val lastIndex = recovery.restoreMemberConsistency(member, onRecoveryFailure = fail())

    val r301 = Request(301, None, request2)
    val r302 = Response(302, None, createResponseMessage(request2))
    val r303 = Response(303, None, createResponseMessage(request3))
    val r304 = Request(304, None, request1)
    val r305 = Response(305, None, createResponseMessage(request1))
    val r306 = Request(306, None, request4)
    val r307 = Response(307, None, createResponseMessage(request4))
    val r308 = Index(308, getMessageTimestamp(request4))
    lastIndex should be(Some(r308))
    txLog.read.toList should be(List(r1, r301, r302, r303, r304, r305, r306, r307, r308))

    verify(mockStore).truncateAt(Consistency.getMessageTimestamp(request5).get, request5.token)

    logDir.list().toSet should be(Set("service-0000001000-1:.log", "service-0000001000-301:.log"))
  }

  test("when rewritten tx timestamps are all smaller than consistent timestamp, finalize with consistent timestamp") {
    val txLog = createTxLog()
    val request1 = createRequestMessage(timestamp = 1, token = 0) // complete
    val request2 = createRequestMessage(timestamp = 2, token = 1) // complete and confirmed by request4
    val request3 = createRequestMessage(timestamp = 3, token = 2) // complete
    val request4 = createRequestMessage(timestamp = 4, token = 0) // incomplete, truncated
    val r1 = txLog.append(Request(1, None, request3))
    val r2 = txLog.append(Request(2, None, request2))
    val r3 = txLog.append(Request(3, None, request1))
    val r4 = txLog.append(Response(4, None, createResponseMessage(request3)))
    val r5 = txLog.append(Response(5, None, createResponseMessage(request2)))
    val r6 = txLog.append(Response(6, None, createResponseMessage(request1)))
    val r7 = txLog.append(Request(7, getMessageTimestamp(request2), request4))
    txLog.commit()

    idGenerator.lastId = 300
    val recovery = new ConsistencyRecovery(logDir.getAbsolutePath, mockStore, idGenerator)
    val lastIndex = recovery.restoreMemberConsistency(member, onRecoveryFailure = fail())

    val r301 = Request(301, None, request2)
    val r302 = Response(302, None, createResponseMessage(request2))
    val r303 = Response(303, None, createResponseMessage(request3))
    val r304 = Request(304, None, request1)
    val r305 = Response(305, None, createResponseMessage(request1))
    val r306 = Index(306, getMessageTimestamp(request2))
    lastIndex should be(Some(r306))
    txLog.read.toList should be(List(r1, r301, r302, r303, r304, r305, r306))

    verify(mockStore).truncateAt(Consistency.getMessageTimestamp(request4).get, request4.token)

    logDir.list().toSet should be(Set("service-0000001000-1:.log", "service-0000001000-301:.log"))
  }

  test("pending recovery log should BE finalized") {
    val recovery = new ConsistencyRecovery(logDir.getAbsolutePath, mockStore, idGenerator)
    val request1 = createRequestMessage(timestamp = 11, token = 0) // complete
    val request2 = createRequestMessage(timestamp = 12, token = 1) // complete and in non finalized recovery log

    // Append 2 complete transactions in member log
    val txLog = createTxLog()
    val r1 = txLog.append(Request(1, None, request1))
    val r2 = txLog.append(Response(2, None, createResponseMessage(request1)))
    val r3 = txLog.append(Request(3, None, request2))
    val r4 = txLog.append(Response(4, None, createResponseMessage(request2)))
    txLog.commit()

    // Create recovery log with the last transaction (split over two log files)
    recovery.recoveryDir.mkdir()
    val recoveryTxLog = createTxLog(dir = recovery.recoveryDir.getAbsolutePath)
    val r201 = recoveryTxLog.append(Request(201, None, request2))
    recoveryTxLog.rollLog()
    val r202 = recoveryTxLog.append(Response(202, None, createResponseMessage(request2)))
    val r203 = recoveryTxLog.append(Index(203, getMessageTimestamp(request2)))
    recoveryTxLog.commit()

    recovery.recoveryDir.list().toSet should be(Set("service-0000001000-201:.log", "service-0000001000-202:.log"))

    // Recovery should simply finalize the pending recover log
    idGenerator.lastId = 500 // larger id seed, to ensure records are not rewritten
    val lastIndex = recovery.restoreMemberConsistency(member, onRecoveryFailure = fail())
    lastIndex should be(Some(r203))

    txLog.read.toList should be(List(r1, r2, r201, r202, r203))
    logDir.list().toSet should be(Set("service-0000001000-1:.log", "service-0000001000-201:.log",
      "service-0000001000-202:.log"))
    verifyZeroInteractions(mockStore)
  }

  test("outdated pending recovery log should fail new recovery attempt") {
    val recovery = new ConsistencyRecovery(logDir.getAbsolutePath, mockStore, idGenerator)
    val request1 = createRequestMessage(timestamp = 11, token = 0) // complete and in non finalized recovery log
    val request2 = createRequestMessage(timestamp = 12, token = 1) // complete in member log

    // Append 2 complete transactions in member log
    val txLog = createTxLog()
    val r1 = txLog.append(Request(1, None, request1))
    val r2 = txLog.append(Response(2, None, createResponseMessage(request1)))
    val r3 = txLog.append(Request(3, None, request2))
    val r4 = txLog.append(Response(4, None, createResponseMessage(request2)))
    val r5 = txLog.append(Index(500, getMessageTimestamp(request1))) // Id greater than recovery log
    txLog.commit()
    txLog.read.toList should be(List(r1, r2, r3, r4, r5))

    // Create recovery log with the first transaction only
    recovery.recoveryDir.mkdir()
    val recoveryTxLog = createTxLog(dir = recovery.recoveryDir.getAbsolutePath)
    recoveryTxLog.append(Request(201, None, request1))
    recoveryTxLog.append(Response(202, None, createResponseMessage(request1)))
    recoveryTxLog.append(Index(203, getMessageTimestamp(request1)))
    recoveryTxLog.commit()

    recovery.recoveryDir.list() should be(Set("service-0000001000-201:.log"))

    evaluating {
      recovery.restoreMemberConsistency(member, onRecoveryFailure = throw new ConsistencyException)
    } should produce[ConsistencyException]

    txLog.read.toList should be(List(r1, r2, r3, r4, r5))
  }

  test("pending recovery log without member log should fail recovery attempt") {
    val recovery = new ConsistencyRecovery(logDir.getAbsolutePath, mockStore, idGenerator)
    val request1 = createRequestMessage(timestamp = 11, token = 0) // only in non finalized recovery log

    // Append 2 complete transactions in member log
    val txLog = createTxLog()
    txLog.read.toList should be(List())

    // Create recovery log with the a transaction
    recovery.recoveryDir.mkdir()
    val recoveryTxLog = createTxLog(dir = recovery.recoveryDir.getAbsolutePath)
    val r201 = recoveryTxLog.append(Request(201, None, request1))
    val r202 = recoveryTxLog.append(Response(202, None, createResponseMessage(request1)))
    val r203 = recoveryTxLog.append(Index(203, getMessageTimestamp(request1)))
    recoveryTxLog.commit()

    recovery.recoveryDir.list() should be(Array("service-0000001000-201:.log"))

    evaluating {
      recovery.restoreMemberConsistency(member, onRecoveryFailure = throw new ConsistencyException)
    } should produce[ConsistencyException]

    txLog.read.toList should be(List())
  }

  test("incomplete pending recovery log should BE cleaned and recovery redone") {
    val recovery = new ConsistencyRecovery(logDir.getAbsolutePath, mockStore, idGenerator)
    val request1 = createRequestMessage(timestamp = 11, token = 0) // complete

    // Append 1 transaction in member log
    val txLog = createTxLog()
    txLog.append(Request(1, None, request1))
    txLog.append(Response(2, None, createResponseMessage(request1)))
    txLog.commit()

    // Create partial recovery log which will be discarted at next recovery attempt
    recovery.recoveryDir.mkdir()
    val recoveryTxLog = createTxLog(dir = recovery.recoveryDir.getAbsolutePath)
    recoveryTxLog.append(Request(201, None, request1))
    recoveryTxLog.commit()

    recovery.recoveryDir.list() should be(Array("service-0000001000-201:.log"))

    // Recovery should simply finalize the pending recover log
    idGenerator.lastId = 500 // larger id seed, to ensure records ARE rewritten
    val lastIndex = recovery.restoreMemberConsistency(member, onRecoveryFailure = fail())

    val r501 = recoveryTxLog.append(Request(501, None, request1))
    val r502 = recoveryTxLog.append(Response(502, None, createResponseMessage(request1)))
    val r503 = recoveryTxLog.append(Index(503, getMessageTimestamp(request1)))

    lastIndex should be(Some(r503))
    txLog.read.toList should be(List(r501, r502, r503))
    logDir.list().toSet should be(Set("service-0000001000-1:.log", "service-0000001000-501:.log"))
    verifyZeroInteractions(mockStore)
  }
}
