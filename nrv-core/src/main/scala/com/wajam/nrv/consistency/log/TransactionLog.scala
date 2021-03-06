package com.wajam.nrv.consistency.log

import com.wajam.nrv.consistency.log.LogRecord.{Response, Index}
import com.wajam.commons.Closable
import com.wajam.nrv.utils.timestamp.Timestamp
import com.wajam.nrv.service.TokenRange

trait TransactionLog {
  /**
   * Returns the most recent consistant timestamp written on the log storage.
   */
  def getLastLoggedRecord: Option[LogRecord]

  /**
   * Appends the specified record to the transaction log
   */
  def append[T <: LogRecord](block: => T): T

  /**
   * Read all the records from the specified index
   */
  def read(index: Index): TransactionLogIterator

  /**
   * Read all the records from the specified request record timestamp. Returns an empty iterator if no request record
   * with the specified timestamp is found.
   */
  def read(timestamp: Timestamp): TransactionLogIterator

  /**
   * Truncate log storage from the specified index inclusively
   */
  def truncate(index: Index)

  /**
   * Ensure that transaction log is fully written on the log storage
   */
  def commit()

  /**
   * Close this transaction log
   */
  def close()

  /**
   * Returns the first timestamped record at the specified timestamp
   */
  def firstRecord(timestamp: Option[Timestamp]): Option[TimestampedRecord] = {
    val itr = timestamp match {
      case Some(ts) => read(ts)
      case None => read(Index(Long.MinValue))
    }
    try {
      itr.collectFirst({
        case record: TimestampedRecord => record
      })
    } finally {
      itr.close()
    }
  }

  /**
   * Returns the most recent successful response timestamp starting at the specified timestamp
   */
  def lastSuccessfulTimestamp(timestamp: Timestamp, ranges: Seq[TokenRange] = Seq(TokenRange.All)): Option[Timestamp] = {
    val itr = read(timestamp).toSafeIterator
    try {
      val responseTimestamps = itr.collect {
        case response: Response if response.status == Response.Success && ranges.exists(_.contains(response.token)) => {
          response.timestamp
        }
      }
      if (responseTimestamps.isEmpty) None else Some(responseTimestamps.max)
    } finally {
      itr.close()
    }
  }
}

trait TransactionLogIterator extends Iterator[LogRecord] with Closable {

  /**
   * Creates a new iterator which stop iterating at the first encountered error.
   * The new iterator read one record ahead and silently eat any resulting exception to stop iteration.
   */
  def toSafeIterator: TransactionLogIterator = {

    val itr = this

    new TransactionLogIterator {

      var nextValue: Option[LogRecord] = getNextValue()

      def hasNext = {
        nextValue.isDefined
      }

      def next() = {
        val value = nextValue
        nextValue = getNextValue()
        value.get
      }

      def close() = itr.close()

      private def getNextValue(): Option[LogRecord] = {
        try {
          if (itr.hasNext) {
            Some(itr.next())
          } else None
        } catch {
          case e: Exception => None
        }
      }
    }
  }
}