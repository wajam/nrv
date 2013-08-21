package com.wajam.nrv.consistency.replication

import com.wajam.nrv.consistency.persistence.{LogRecord, TransactionLogIterator, TimestampedRecord, TransactionLog}
import com.wajam.nrv.utils.timestamp.Timestamp
import collection.immutable.TreeMap
import com.wajam.nrv.consistency.persistence.LogRecord.{Index, Response, Request}
import com.wajam.nrv.consistency.ResolvedServiceMember
import com.yammer.metrics.scala.Instrumented

/**
 * Replication source iterator backed by a transaction log. The transactions are ordered by timestamp. This iterator
 * only returns successful transactions. The iterator does not returns transactions beyond the current consistent
 * timestamp. If more records are available beyond current consistent timestamp, the iterator hasMore() continue
 * to return true but next() returns None. New transactions will be returned if new records are written to the
 * log and the current consistent timestamp advance. If the consistent timestamp advance beyond the end of the log,
 * the iterator is closed and hasNext() returns false.
 * <p><p>
 * This class is not thread safe and must invoked from a single thread or synchronized externaly.
 */
class TransactionLogReplicationIterator(member: ResolvedServiceMember, val start: Timestamp,
                                        txLog: TransactionLog, currentConsistentTimestamp: => Option[Timestamp],
                                        mustDrain: => Boolean = false)
  extends ReplicationSourceIterator with Instrumented {

  val end = None
  private var lastReadRecord: Option[LogRecord] = None
  private var pendingTransactions: TreeMap[Timestamp, PendingTransaction] = TreeMap()
  private val itr = initLogIterator()
  private var lastTxTimestamp: Option[Long] = None

  lazy private val nextEmptyMeter = metrics.meter("next-empty", "next-empty", member.scopeName)
  lazy private val nextDefinedMeter = metrics.meter("next-defined", "next-defined", member.scopeName)
  private val pendingTransactionsSizeGauge = metrics.gauge("pending-tx-size", member.scopeName) {
    pendingTransactions.size
  }
  private val lastTransactionTimestampGauge = metrics.gauge("last-tx-timestamp", member.scopeName) {
    lastTxTimestamp.getOrElse(0L)
  }

  def hasNext = {
    readMoreLogRecords()
    itr.hasNext || (!mustDrain && lastRecordIsIndexEqualsToCurrentConsistentTimestamp) ||
      (mustDrain && pendingTransactions.nonEmpty)
  }

  def next() = {
    val nextTx = if (isHeadTransactionReady) {
      val (timestamp, PendingTransaction(request, Some(response))) = pendingTransactions.head
      pendingTransactions -= timestamp
      if (response.isSuccess) {
        val message = request.message
        message.timestamp = Some(timestamp)
        lastTxTimestamp = Some(request.timestamp.value)
        Some(message)
      } else {
        None
      }
    } else {
      None
    }

    if (nextTx.isEmpty) {
      nextEmptyMeter.mark()
    } else {
      nextDefinedMeter.mark()
    }
    nextTx
  }

  def close() {
    itr.close()
  }

  private case class PendingTransaction(request: Request, var response: Option[Response] = None)

  /**
   * Returns true if the head transaction is complete (i.e. have a response) and its timestamp is not beyond
   * the last read record consistent timestamp.
   */
  private def isHeadTransactionReady: Boolean = {
    if (mustDrain && !itr.hasNext && pendingTransactions.nonEmpty) {
      true
    } else {
      val ready = for {
        (_, tx) <- pendingTransactions.headOption
        headResponse <- tx.response
        maxTimestamp <- currentConsistentTimestamp
      } yield headResponse.timestamp <= maxTimestamp
      ready.getOrElse(false)
    }
  }

  private def lastRecordIsIndexEqualsToCurrentConsistentTimestamp: Boolean = {
    lastReadRecord match {
      case Some(lastIndex: Index) => lastIndex.consistentTimestamp == currentConsistentTimestamp
      case _ => false
    }
  }

  /**
   * Returns true if the last read record timestamp is not beyond the current consistent timestamp
   */
  private def lastRecordIsBeforeOrEqualsToCurrentConsistentTimestamp(indexCanEqualsConsistentTimestamp: Boolean): Boolean = {
    (currentConsistentTimestamp, lastReadRecord) match {
      case (Some(maxTimestamp), Some(lastRecord: TimestampedRecord)) if lastRecord.timestamp <= maxTimestamp => true
      case (Some(maxTimestamp), Some(Index(_, Some(lastIndexConsistentTimestamp)))) => {
        lastIndexConsistentTimestamp < maxTimestamp ||
          (indexCanEqualsConsistentTimestamp && lastIndexConsistentTimestamp == maxTimestamp)
      }
      case _ => false
    }
  }

  private def readMoreLogRecords() {
    // Read records until the head transaction is ready (see definition above) but never going beyond the the current
    // consistent timestamp.
    while (itr.hasNext && (lastReadRecord.isEmpty || !isHeadTransactionReady &&
      (mustDrain && lastRecordIsBeforeOrEqualsToCurrentConsistentTimestamp(indexCanEqualsConsistentTimestamp = true) ||
        lastRecordIsBeforeOrEqualsToCurrentConsistentTimestamp(indexCanEqualsConsistentTimestamp = false)))) {
      val record = itr.next()
      record match {
        case request: Request => {
          pendingTransactions += (request.timestamp -> PendingTransaction(request))
          lastReadRecord = Some(request)
        }
        case response: Response => {
          pendingTransactions(response.timestamp).response = Some(response)
          lastReadRecord = Some(response)
        }
        case index: Index => lastReadRecord = Some(index)
      }
    }
  }

  private def initLogIterator(): TransactionLogIterator = {
    val firstRecord: Option[TimestampedRecord] = try {
      txLog.firstRecord(Some(start))
    } catch {
      case e: NoSuchElementException => {
        // Ignore the error if the log is empty, an empty iterator will be returned
        if (txLog.firstRecord(None).isEmpty) None else throw e
      }
    }

    firstRecord match {
      case Some(record) => {
        // Since the records can be written out of order, we need to read from the record consistent
        // timestamp and filter out records with timestamp prior the record timestamp.
        val rawItr = record.consistentTimestamp match {
          case Some(startTimestamp) => txLog.read(startTimestamp)
          case None => txLog.read(Index(Long.MinValue))
        }
        new TransactionLogIterator {
          val filteredItr = rawItr.withFilter(_ match {
            case r: TimestampedRecord if r.timestamp >= record.timestamp => true
            case i: Index => true
            case _ => false
          })

          def hasNext = filteredItr.hasNext

          def next() = filteredItr.next()

          def close() {
            rawItr.close()
          }
        }
      }
      case None => {
        // No starting record, read log from the beginning
        txLog.read(Index(Long.MinValue))
      }
    }
  }
}
