package com.wajam.nrv.consistency.replication

import com.wajam.nrv.consistency.persistence.{TransactionLogIterator, TimestampedRecord, TransactionLog}
import com.wajam.nrv.utils.timestamp.Timestamp
import collection.immutable.TreeMap
import com.wajam.nrv.consistency.persistence.LogRecord.{Index, Response, Request}
import com.wajam.nrv.consistency.{ResolvedServiceMember, Consistency}
import com.wajam.nrv.Logging
import com.yammer.metrics.scala.Instrumented

/**
 * Replication source iterator backed by a transaction log. The transactions are ordered by timestamp. This iterator
 * only returns successful transactions. The iterator does not returns transactions beyond the current consistent
 * timestamp. If more records are available * beyond current consistent timestamp, the iterator hasMore() continue
 * to return true but next() returns None. New transactions will be returned if new records are written to the
 * log and the current consistent timestamp advance. If the consistent timestamp advance beyond the end of the log,
 * the iterator is closed and hasNext() returns false.
 * <p><p>
 * This class is not thread safe and must invoked from a single thread or synchronized externaly.
 */
class TransactionLogReplicationIterator(member: ResolvedServiceMember, val from: Timestamp,
                                        txLog: TransactionLog, currentConsistentTimestamp: => Option[Timestamp])
  extends ReplicationSourceIterator with Instrumented {

  val to = None
  private var lastReadRecord: Option[TimestampedRecord] = None
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
    itr.hasNext
  }

  def next() = {
    val nextTx = if (isHeadTransactionReady) {
      val (timestamp, PendingTransaction(request, Some(response))) = pendingTransactions.head
      pendingTransactions -= timestamp
      if (response.isSuccess) {
        val message = request.message
        Consistency.setMessageTimestamp(message, timestamp)
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
   * Returns true if the head transaction is complete (i.e. doesn't have a response) and its timestamp is not beyond
   * the last read record consistent timestamp.
   */
  private def isHeadTransactionReady: Boolean = {
    val ready = for {
      (_, tx) <- pendingTransactions.headOption
      headResponse <- tx.response
      lastRecord <- lastReadRecord
      maxTimestamp <- lastRecord.consistentTimestamp
    } yield headResponse.timestamp <= maxTimestamp
    ready.getOrElse(false)
  }

  /**
   * Returns true if the last read record timestamp is not beyond the current consistent timestamp
   */
  private def isLastRecordBeforeCurrentConsistentTimestamp: Boolean = {
    val before = for {
      maxTimestamp <- currentConsistentTimestamp
      lastRecord <- lastReadRecord
    } yield lastRecord.timestamp < maxTimestamp
    before.getOrElse(false)
  }

  private def readMoreLogRecords() {
    // Read records as long the head transaction is pending and the last record read is not
    // beyond the current consistent timestamp
    while (itr.hasNext && (lastReadRecord.isEmpty ||
      isLastRecordBeforeCurrentConsistentTimestamp && !isHeadTransactionReady)) {
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
        case index: Index => // Just skip index
      }
    }
  }

  private def initLogIterator(): TransactionLogIterator = {
    txLog.firstRecord(Some(from)) match {
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
        // No starting record, read log from the begining
        txLog.read(Index(Long.MinValue))
      }
    }
  }
}
