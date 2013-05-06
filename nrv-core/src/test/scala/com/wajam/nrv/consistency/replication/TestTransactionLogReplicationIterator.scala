package com.wajam.nrv.consistency.replication

import org.scalatest.BeforeAndAfter
import com.wajam.nrv.consistency.{TransactionRecorder, ResolvedServiceMember, TestTransactionBase}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import java.io.File
import com.wajam.nrv.consistency.persistence.{LogRecord, FileTransactionLog}
import java.nio.file.Files
import org.scalatest.matchers.ShouldMatchers._
import com.wajam.nrv.consistency.persistence.LogRecord.Index
import com.wajam.nrv.utils.timestamp.Timestamp
import com.wajam.nrv.service.TokenRange
import com.wajam.nrv.utils.{Future, IdGenerator}
import com.wajam.nrv.data.Message
import scala.annotation.tailrec
import scala.util.Random

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

  test("empty log should not fail") {
    val itr = new TransactionLogReplicationIterator(member, start = 1L, txLog, Some(Long.MaxValue))
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
      itr.take(takeCount).flatten.map(msg => msg.timestamp).flatten.map(ts => ts.value).toList
    }

    val from1 = new TransactionLogReplicationIterator(member, start = 1L, txLog, currentConsistentTimestamp)
    takeTimestamps(from1, consistentTimestamp = None, 100) should be(List())
    takeTimestamps(from1, consistentTimestamp = Some(1), 100) should be(List())
    takeTimestamps(from1, consistentTimestamp = Some(5), 100) should be(List(1L))
    takeTimestamps(from1, consistentTimestamp = Some(5), 100) should be(List())
    takeTimestamps(from1, consistentTimestamp = Some(7), 100) should be(List(2L, 4L, 5L, 6L))
    takeTimestamps(from1, consistentTimestamp = Some(7), 100) should be(List())
    takeTimestamps(from1, consistentTimestamp = Some(8), 100) should be(List(7L))
    takeTimestamps(from1, consistentTimestamp = Some(8), 100) should be(List())

    val from3 = new TransactionLogReplicationIterator(member, start = 3L, txLog, currentConsistentTimestamp)
    takeTimestamps(from3, consistentTimestamp = None, 100) should be(List())
    takeTimestamps(from3, consistentTimestamp = Some(8), 100) should be(List(4L, 5L, 6L, 7L))
    takeTimestamps(from3, consistentTimestamp = Some(8), 100) should be(List())

    val from6 = new TransactionLogReplicationIterator(member, start = 6L, txLog, currentConsistentTimestamp)
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

    // Missing timestamp
    evaluating {
      new TransactionLogReplicationIterator(member, start = -1L, txLog, None)
    } should produce[NoSuchElementException]
  }

  /**
   * This test append and read new log records concurently from two threads and ensure that the iterator is not closed
   * when log rolls.
   */
  test("iterator not closed on log roll") {

    // Setup a transaction recorder which take care up computing the consistent timestamp. The recorder time and
    // id generator are both bound to a local variable which is incremented every time a new message is appended.
    // This make the test more deterministic and the time management does not requires any sleep.
    var consistencyErrorCount = 0
    var currentId = 0L
    val recorder = new TransactionRecorder(member, txLog, consistencyDelay = 500L, consistencyTimeout = 5000L,
      commitFrequency = 0, onConsistencyError = consistencyErrorCount += 1,
      idGenerator = new IdGenerator[Long] {
        def nextId = currentId
      }) {
      override def currentTime = currentId
    }
    recorder.start()

    def appendMessage(message: Message) {
      currentId = currentId + 10
      recorder.checkPending()
      recorder.appendMessage(message)
      recorder.checkPending()
    }

    // Append a deterministic initial transaction in log. This is where the consumer will start reading.
    val request1 = createRequestMessage(timestamp = 0)
    appendMessage(request1)
    appendMessage(createResponseMessage(request1))

    // Appender which append transaction until reaching a maximum count. The consumer is concurently reading the
    // transaction log as they are appended.
    val maxTxCount = 200
    val appender = Future {
      @tailrec
      def append(appendedCount: Int): Int = {
        if (appendedCount < maxTxCount) {
          val requests = (1 to Random.nextInt(10 + 1)).map(i => createRequestMessage(timestamp = (i + appendedCount)))
          val responses = requests.map(r => createResponseMessage(r))

          Random.shuffle(requests).foreach(appendMessage(_))
          txLog.rollLog()
          Random.shuffle(responses).foreach(appendMessage(_))

          append(appendedCount + requests.size)
        } else {
          appendedCount
        }
      }

      append(0)
    }

    // Wait for a consistent timestamp before consuming replication source
    while (recorder.currentConsistentTimestamp == None) {}

    // Consumer is reading from the transaction log while new transaction are added by the appender until the appender
    // completed and the consumer read last record repecting the last appended record consistent timestamp.
    val consumer = Future {
      val itr = new TransactionLogReplicationIterator(member, start = request1.timestamp.get,
        txLog, recorder.currentConsistentTimestamp)

      def finalConsistentRecord = txLog.firstRecord(txLog.getLastLoggedRecord.get.consistentTimestamp)

      def isFinalTx(txOpt: Option[Message]) = {
        txOpt match {
          case Some(tx) if appender.isCompleted => tx.timestamp == finalConsistentRecord.get.consistentTimestamp
          case _ => false
        }
      }

      // Consume until end of iterator (not expected) or the final transaction is consumed.
      @tailrec
      def consume(last: Option[Message]): Option[Message] = {
        if (itr.hasNext && !isFinalTx(last)) {
          itr.next() match {
            case tx@Some(_) => consume(tx)
            case None => consume(last)
          }
        } else {
          last
        }
      }

      consume(None)
    }

    // Wait for the appender and consumer to complete
    val appendedCount = Future.blocking(appender, 5000L)
    appendedCount should be >= maxTxCount

    val lastConsumed = Future.blocking(consumer, 5000L)
    val expectedLastTx = txLog.firstRecord(txLog.getLastLoggedRecord.get.consistentTimestamp)
    expectedLastTx should not be (None)
    lastConsumed.get.timestamp should be(expectedLastTx.get.consistentTimestamp)

    consistencyErrorCount should be(0)
  }
}
