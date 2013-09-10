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
import com.wajam.nrv.utils.IdGenerator
import com.wajam.nrv.data.Message
import scala.annotation.tailrec
import scala.util.Random
import scala.concurrent.{Future, Await}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

@RunWith(classOf[JUnitRunner])
class TestTransactionLogReplicationIterator extends TestTransactionBase with BeforeAndAfter {
  var member: ResolvedServiceMember = null
  var logDir: File = null
  var txLog: FileTransactionLog = null

  before {
    logDir = Files.createTempDirectory("TestTransactionLogReplicationIterator").toFile
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
    val itr = new TransactionLogReplicationIterator(member, start = 1L, txLog, Some(Long.MaxValue), isDraining = false)
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

    val from1 = new TransactionLogReplicationIterator(member, start = 1L, txLog, currentConsistentTimestamp, isDraining = false)
    takeTimestamps(from1, consistentTimestamp = None, 100) should be(List())
    takeTimestamps(from1, consistentTimestamp = Some(1), 100) should be(List())
    takeTimestamps(from1, consistentTimestamp = Some(5), 100) should be(List(1L))
    takeTimestamps(from1, consistentTimestamp = Some(5), 100) should be(List())
    takeTimestamps(from1, consistentTimestamp = Some(7), 100) should be(List(2L, 4L, 5L, 6L, 7L))
    takeTimestamps(from1, consistentTimestamp = Some(7), 100) should be(List())
    takeTimestamps(from1, consistentTimestamp = Some(8), 100) should be(List(8L))
    takeTimestamps(from1, consistentTimestamp = Some(8), 100) should be(List())

    val from3 = new TransactionLogReplicationIterator(member, start = 3L, txLog, currentConsistentTimestamp, isDraining = false)
    takeTimestamps(from3, consistentTimestamp = None, 100) should be(List())
    takeTimestamps(from3, consistentTimestamp = Some(8), 100) should be(List(4L, 5L, 6L, 7L, 8L))
    takeTimestamps(from3, consistentTimestamp = Some(8), 100) should be(List())

    val from6 = new TransactionLogReplicationIterator(member, start = 6L, txLog, currentConsistentTimestamp, isDraining = false)
    takeTimestamps(from6, consistentTimestamp = None, 100) should be(List())
    takeTimestamps(from6, consistentTimestamp = Some(8), 100) should be(List(6L, 7L, 8L))
    takeTimestamps(from6, consistentTimestamp = Some(8), 100) should be(List())

    // Append more records and read them
    txLog.append(LogRecord(1000, Some(8), request9))
    txLog.append(LogRecord(1010, Some(8), createResponseMessage(request9)))
    txLog.append(Index(1020, Some(9)))

    takeTimestamps(from1, consistentTimestamp = Some(9), 100) should be(List(9L))
    from1.hasNext should be(true)
    takeTimestamps(from3, consistentTimestamp = Some(9), 100) should be(List(9L))
    from3.hasNext should be(true)
    takeTimestamps(from6, consistentTimestamp = Some(9), 100) should be(List(9L))
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
      new TransactionLogReplicationIterator(member, start = -1L, txLog, None, isDraining = false)
    } should produce[NoSuchElementException]
  }

  /**
   * This test append and read new log records concurrently from two threads and ensure that the iterator is not closed
   * when log rolls.
   */
  test("iterator not closed on log roll") {

    // Setup a transaction recorder which take care up computing the consistent timestamp. The recorder time and
    // id generator are both bound to a local variable which is incremented every time a new message is appended.
    // This make the test more deterministic and the time management does not requires any sleep.
    var consistencyErrorCount = 0
    var currentId = 0L
    val consistencyDelay = 500L
    val recorder = new TransactionRecorder(member, txLog, consistencyDelay, consistencyTimeout = 5000L,
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

    // Appender which append transaction until reaching a maximum count. The consumer is concurrently reading the
    // transaction log as they are appended.
    val maxTxCount = 200
    val appender = Future {
      @tailrec
      def append(appendedCount: Int): Int = {
        if (appendedCount < maxTxCount) {
          val requests = (1 to Random.nextInt(10 + 1)).map(i => createRequestMessage(timestamp = i + appendedCount))
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
    // completed and the consumer read last record respecting the last appended record consistent timestamp.
    val consumer = Future {
      val itr = new TransactionLogReplicationIterator(member, start = request1.timestamp.get,
        txLog, recorder.currentConsistentTimestamp, isDraining = false)

      def finalConsistentRecord = {
        txLog.getLastLoggedRecord.collectFirst {
          case i: Index if i.consistentTimestamp == recorder.currentConsistentTimestamp => i
        }
      }

      def isFinalTx(txOpt: Option[Message]) = {
        if (appender.isCompleted) {
          (txOpt, finalConsistentRecord) match {
            case (Some(tx), Some(finalRecord)) => tx.timestamp == finalRecord.consistentTimestamp
            case _ => false
          }
        } else false
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
    val appendedCount = Await.result(appender, 5.seconds)
    appendedCount should be >= maxTxCount
    // Update current timestamp past consistency delay to ensure a final Index record is appended
    currentId = appendedCount * 2 * 10 + consistencyDelay

    // Consume all appended transactions
    val lastConsumed = Await.result(consumer, 5.seconds)
    lastConsumed.get.timestamp should be(Some(Timestamp(appendedCount)))

    consistencyErrorCount should be(0)
  }

  test("should returns all transactions when draining") {
    val request1 = createRequestMessage(timestamp = 1)
    val request2 = createRequestMessage(timestamp = 2)
    val request3 = createRequestMessage(timestamp = 3)
    val request4 = createRequestMessage(timestamp = 4)

    txLog.append(LogRecord(110, None, request1))
    txLog.append(LogRecord(120, None, createResponseMessage(request1)))
    txLog.append(LogRecord(130, Some(1), request2))
    txLog.append(LogRecord(140, Some(1), createResponseMessage(request2)))
    txLog.append(LogRecord(150, Some(2), request3))
    txLog.append(LogRecord(160, Some(2), createResponseMessage(request3)))
    txLog.append(LogRecord(170, Some(3), request4))
    txLog.append(LogRecord(180, Some(3), createResponseMessage(request4)))
    txLog.append(Index(190, Some(3)))

    var draining = false
    var currentConsistentTimestamp: Option[Timestamp] = Some(3L)

    val itr = new TransactionLogReplicationIterator(member, start = 1L, txLog, currentConsistentTimestamp, isDraining = draining)
    itr.hasNext should be(true)
    itr.next().flatMap(_.timestamp) should be(Some(Timestamp(1L)))
    itr.hasNext should be(true)
    itr.next().flatMap(_.timestamp) should be(Some(Timestamp(2L)))
    itr.hasNext should be(true)
    itr.next().flatMap(_.timestamp) should be(Some(Timestamp(3L)))
    itr.hasNext should be(true)

    // Verify not reading beyond consistent timestamp even when draining
    draining = true
    itr.hasNext should be(true)
    itr.next() should be(None)

    // Verify iterator is closed when draining AND last record timestamp IS the consistent timestamp
    currentConsistentTimestamp = Some(4L)
    itr.hasNext should be(true)
    itr.next().flatMap(_.timestamp) should be(Some(Timestamp(4L)))
    itr.hasNext should be(false)
  }

  test("should returns transactions in proper order when older tx start after complete tx") {
    val request1 = createRequestMessage(timestamp = 1)
    val request2 = createRequestMessage(timestamp = 2)
    val request3 = createRequestMessage(timestamp = 3)
    val request4 = createRequestMessage(timestamp = 4)

    txLog.append(LogRecord(110, None, request1))
    txLog.append(LogRecord(120, None, createResponseMessage(request1)))
    txLog.append(LogRecord(130, None, request4))
    txLog.append(LogRecord(140, None, createResponseMessage(request4)))
    txLog.append(LogRecord(150, Some(1L), request2))
    txLog.append(LogRecord(160, Some(1L), createResponseMessage(request2)))
    txLog.append(LogRecord(170, Some(1L), request3))
    txLog.append(LogRecord(180, Some(1L), createResponseMessage(request3)))
    txLog.append(Index(190, Some(4)))


    var currentConsistentTimestamp: Option[Timestamp] = None

    val itr = new TransactionLogReplicationIterator(member, start = 1L, txLog, currentConsistentTimestamp, isDraining = false)
    itr.take(100).flatten.flatMap(_.timestamp).map(_.value).toList should be(List())
    currentConsistentTimestamp = Some(1L)
    itr.take(100).flatten.flatMap(_.timestamp).map(_.value).toList should be(List())
    currentConsistentTimestamp = Some(4L)
    itr.take(100).flatten.flatMap(_.timestamp).map(_.value).toList should be(List(1L, 2L, 3L, 4L))
  }
}
