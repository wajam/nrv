package com.wajam.nrv.consistency.replication

import org.scalatest.BeforeAndAfter
import com.wajam.nrv.consistency.{ResolvedServiceMember, Consistency, TestTransactionBase}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import java.io.File
import com.wajam.nrv.consistency.persistence.{LogRecord, FileTransactionLog}
import java.nio.file.Files
import org.scalatest.matchers.ShouldMatchers._
import com.wajam.nrv.consistency.persistence.LogRecord.{Index, Response, Request}
import com.wajam.nrv.utils.timestamp.Timestamp
import com.wajam.nrv.data.Message
import com.wajam.nrv.service.TokenRange

@RunWith(classOf[JUnitRunner])
class TestTransactionLogReplicationIterator extends TestTransactionBase with BeforeAndAfter {
  var member: ResolvedServiceMember = null
  var logDir: File = null
  var txLog: FileTransactionLog = null

  before {
    logDir = Files.createTempDirectory("TestFileTransactionLog").toFile
    member = ResolvedServiceMember(serviceName = "service", token = 1000L, ranges = Seq(TokenRange.All))
    txLog = new FileTransactionLog(member.serviceName, member.token, logDir = logDir.getAbsolutePath)
  }

  after {
    member = null

    txLog.close()
    txLog = null

    logDir.listFiles().foreach(_.delete())
    logDir.delete()
    logDir = null
  }

  test("empty log") {
    val itr = new TransactionLogReplicationIterator(member, from = 1L, txLog, Some(Long.MaxValue))
    itr.hasNext should be(false)
  }

  test("should returns expected transactions as the consistentTimestamp advance and new records are written in log") {
    val request1 = createRequestMessage(timestamp = 1)
    val request2 = createRequestMessage(timestamp = 2)
    val request3 = createRequestMessage(timestamp = 3)
    val request4 = createRequestMessage(timestamp = 4)
    val request5 = createRequestMessage(timestamp = 5)
    val request6 = createRequestMessage(timestamp = 6)
    val request7 = createRequestMessage(timestamp = 7)
    val request8 = createRequestMessage(timestamp = 8)
    val request9 = createRequestMessage(timestamp = 9)

    txLog.append(LogRecord(110, None, request1))
    txLog.append(LogRecord(120, None, request3))
    txLog.append(LogRecord(130, None, request4))
    txLog.append(LogRecord(140, None, createResponseMessage(request4)))
    txLog.append(LogRecord(150, None, createResponseMessage(request1)))
    txLog.append(LogRecord(160, None, request2))
    txLog.append(LogRecord(170, Some(1), request6))
    txLog.append(LogRecord(190, Some(1), createResponseMessage(request2)))
    txLog.append(LogRecord(210, Some(1), request5))
    txLog.append(LogRecord(220, Some(1), createResponseMessage(request3, error = Some(new Exception))))
    txLog.append(LogRecord(230, Some(4), createResponseMessage(request5)))
    txLog.append(LogRecord(240, Some(4), createResponseMessage(request6)))
    txLog.append(LogRecord(250, Some(6), request7))
    txLog.append(LogRecord(260, Some(6), createResponseMessage(request7)))
    txLog.append(LogRecord(270, Some(7), request8))
    txLog.append(LogRecord(280, Some(7), createResponseMessage(request8)))
    txLog.append(Index(290, Some(8)))

    var currentConsistentTimestamp: Option[Timestamp] = None
    def takeTimestamps(itr: TransactionLogReplicationIterator, consistentTimestamp: Option[Timestamp], takeCount: Int): List[Long] = {
      currentConsistentTimestamp = consistentTimestamp
      itr.take(takeCount).flatten.map(msg => Consistency.getMessageTimestamp(msg)).flatten.map(ts => ts.value).toList
    }

    val from1 = new TransactionLogReplicationIterator(member, from = 1L, txLog, currentConsistentTimestamp)
    takeTimestamps(from1, consistentTimestamp = None, 100) should be(List())
    takeTimestamps(from1, consistentTimestamp = Some(1), 100) should be(List())
    takeTimestamps(from1, consistentTimestamp = Some(5), 100) should be(List(1L))
    takeTimestamps(from1, consistentTimestamp = Some(5), 100) should be(List())
    takeTimestamps(from1, consistentTimestamp = Some(7), 100) should be(List(2L, 4L, 5L, 6L))
    takeTimestamps(from1, consistentTimestamp = Some(7), 100) should be(List())
    takeTimestamps(from1, consistentTimestamp = Some(8), 100) should be(List(7L))
    takeTimestamps(from1, consistentTimestamp = Some(8), 100) should be(List())

    val from3 = new TransactionLogReplicationIterator(member, from = 3L, txLog, currentConsistentTimestamp)
    takeTimestamps(from3, consistentTimestamp = None, 100) should be(List())
    takeTimestamps(from3, consistentTimestamp = Some(8), 100) should be(List(4L, 5L, 6L, 7L))
    takeTimestamps(from3, consistentTimestamp = Some(8), 100) should be(List())

    val from6 = new TransactionLogReplicationIterator(member, from = 6L, txLog, currentConsistentTimestamp)
    takeTimestamps(from6, consistentTimestamp = None, 100) should be(List())
    takeTimestamps(from6, consistentTimestamp = Some(8), 100) should be(List(6L, 7L))
    takeTimestamps(from6, consistentTimestamp = Some(8), 100) should be(List())

    // Append more records and read them
    txLog.append(LogRecord(1000, Some(8), request9))
    txLog.append(LogRecord(1010, Some(8), createResponseMessage(request9)))
    txLog.append(Index(1020, Some(9)))

    takeTimestamps(from1, consistentTimestamp = Some(9), 100) should be(List(8L))
    from1.hasNext should be(true)
    takeTimestamps(from3, consistentTimestamp = Some(9), 100) should be(List(8L))
    from3.hasNext should be(true)
    takeTimestamps(from6, consistentTimestamp = Some(9), 100) should be(List(8L))
    from6.hasNext should be(true)

    // Try read beyond the end of log
    takeTimestamps(from1, consistentTimestamp = Some(Long.MaxValue), 100) should be(List())
    from1.hasNext should be(false)
    takeTimestamps(from3, consistentTimestamp = Some(Long.MaxValue), 100) should be(List())
    from1.hasNext should be(false)
    takeTimestamps(from6, consistentTimestamp = Some(Long.MaxValue), 100) should be(List())
    from1.hasNext should be(false)
  }
}
