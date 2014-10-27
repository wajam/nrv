package com.wajam.nrv.consistency

import scala.actors.Actor
import com.yammer.metrics.scala.Instrumented
import com.wajam.nrv.consistency.log.LogRecord.{Request, Response, Index}
import com.wajam.nrv.consistency.log.TransactionLog
import com.wajam.nrv.data.{MessageType, Message}
import util.Random
import com.wajam.commons.Logging
import collection.immutable.TreeMap
import annotation.tailrec
import com.wajam.commons.{CurrentTime, IdGenerator}
import com.wajam.nrv.utils.{TimestampIdGenerator, Scheduler}
import com.wajam.nrv.utils.timestamp.Timestamp

class TransactionRecorder(val member: ResolvedServiceMember, val txLog: TransactionLog,
                          consistencyDelay: Long, consistencyTimeout: Long, commitFrequency: Int,
                          onConsistencyError: => Unit, idGenerator: IdGenerator[Long] = new TimestampIdGenerator)
  extends CurrentTime with Instrumented with Logging {

  lazy private val consistencyErrorDuplicate = metrics.meter("consistency-error-duplicate", "consistency-error-duplicate")
  lazy private val consistencyErrorAppend = metrics.meter("consistency-error-append", "consistency-error-append")
  lazy private val consistencyErrorRequest = metrics.meter("consistency-error-request", "consistency-error-request")
  lazy private val consistencyErrorResponse = metrics.meter("consistency-error-response", "consistency-error-response")
  lazy private val consistencyErrorUnexpectedResponse = metrics.meter("consistency-error-unexpected-response",
    "consistency-error-unexpected-response")
  lazy private val consistencyErrorResponseMissingTimestamp = metrics.meter("consistency-error-response-missing-timestamp",
    "consistency-error-response-missing-timestamp")
  lazy private val consistencyErrorCommit = metrics.meter("consistency-error-error-commit", "consistency-error-commit")
  lazy private val consistencyErrorTimeout = metrics.meter("consistency-error-timeout", "consistency-error-timeout")
  lazy private val consistencyErrorCheckPending = metrics.meter("consistency-error-check-pending",
    "consistency-error-check-pending")
  lazy private val unexpectedFailResponse = metrics.meter("unexpected-fail-response", "unexpected-fail-response")
  lazy private val killError = metrics.meter("kill-error", "kill-error")
  lazy private val outdatedIndexSkip = metrics.meter("outdated-index-skip", "outdated-index-skip")

  private val consistencyActor = new ConsistencyActor
  private var currentMaxTimestamp: Timestamp = Long.MinValue
  private var consistencyError: Option[ConsistencyException] = None

  def queueSize = consistencyActor.queueSize

  def pendingSize = consistencyActor.pendingSize

  private val lastWrittenConsistentTimestamp = new AtomicTimestamp(
    AtomicTimestamp.updateIfGreater, consistencyActor.consistentTimestamp)

  def currentConsistentTimestamp = lastWrittenConsistentTimestamp.get

  def start() {
    consistencyActor.start()
  }

  def kill() {
    consistencyActor.stop()
  }

  def appendMessage(message: Message) {
    message.function match {
      case MessageType.FUNCTION_CALL => {
        appendRequest(message)
      }
      case MessageType.FUNCTION_RESPONSE => {
        appendResponse(message)
      }
    }
  }

  private def appendRequest(message: Message) {
    try {
      // The id generation and max timestamp computation are synchronized inside the append method implementation
      var requestMaxTimestamp: Timestamp = null
      val request = txLog.append {
        validateConsistency()
        val request = Request(idGenerator.nextId, consistencyActor.consistentTimestamp, message)
        requestMaxTimestamp = if (currentMaxTimestamp > request.timestamp) currentMaxTimestamp else request.timestamp
        currentMaxTimestamp = requestMaxTimestamp
        request
      }
      consistencyActor ! RequestAppended(request, requestMaxTimestamp)
      lastWrittenConsistentTimestamp.update(request.consistentTimestamp)
    } catch {
      case e: Exception => {
        consistencyErrorAppend.mark()
        warn("Error appending request message {}. {}", message, e)
        val ce = handleConsistencyError(Some(e))
        throw ce
      }
    }
  }

  private def appendResponse(message: Message) {
    try {
      message.timestamp match {
        case Some(timestamp) => {
          // The id generation is synchronized inside the append method implementation
          val response = txLog.append {
            validateConsistency()
            Response(idGenerator.nextId, consistencyActor.consistentTimestamp, message)
          }
          consistencyActor ! ResponseAppended(response)
          lastWrittenConsistentTimestamp.update(response.consistentTimestamp)
        }
        case None if isMessageSuccessful(message) => {
          consistencyErrorResponseMissingTimestamp.mark()
          error("Response is sucessful but missing timestamp {}. ", message)
          handleConsistencyError()
        }
        case None => {
          // Ignore failure response without timestamp, they are SCN response error
          debug("Response is not successful and missing timestamp {}. ", message)
        }
      }
    } catch {
      case e: Exception => {
        consistencyErrorAppend.mark()
        warn("Error appending response message {}. {}", message, e)
        val ce = handleConsistencyError(Some(e))
        throw ce
      }
    }
  }

  private def appendIdleIndex(consistentTimestamp: Timestamp) {

    class OutdatedIndexException extends Exception

    try {
      val index = txLog.append {
        // The id generation is synchronized inside the append method implementation
        validateConsistency()

        if (currentMaxTimestamp > consistentTimestamp) {
          // Never append an Index if a record with a timestamp greater than the consistent timestamp has been appended
          // previously. The OutdatedIndexException does not compromise the consistency (quite the opposite), we just
          // want to skip the Index and continue. This works because currentMaxTimestamp is only accessed from inside
          // the append method which is synchronized by the transaction log implementation.
          //
          // WHY this protection?
          // If Index is the final record of a transaction log, the recovery logic assume the log is consistent with the
          // storage and the recovery process is skip. Imagine this twisted scenario: A new Request is appended
          // concurrently but just a microsecond before the idle Index. The member becomes inconsistent after the
          // store is updated but before the Response is written in the log. The transaction log is now inconsistent
          // with the store but the recovery will be skip because the transaction log is terminated an Index record!
          info("Skip oudated idle Index. indexCTS={}, maxTs={}", consistentTimestamp, currentMaxTimestamp)
          throw new OutdatedIndexException
        }
        Index(idGenerator.nextId, Some(consistentTimestamp))
      }
      lastWrittenConsistentTimestamp.update(index.consistentTimestamp)
    } catch {
      case _: OutdatedIndexException => {
        outdatedIndexSkip.mark()
      }
    }
  }

  private def validateConsistency() {
    consistencyError match {
      case Some(e) => {
        val ce = new ConsistencyException
        ce.initCause(e)
        throw ce
      }
      case None =>
    }
  }

  private def handleConsistencyError(e: Option[Exception] = None): ConsistencyException = {
    // Synchronized with the transaction log to prevent new transaction to be recorded
    txLog.synchronized {
      consistencyError match {
        case Some(ce) => ce
        case None => {
          // Initialize the original consistency error and its optional cause.
          val ce = e match {
            case Some(cause) => {
              val ce = new ConsistencyException
              ce.initCause(cause)
              ce
            }
            case None => new ConsistencyException
          }

          consistencyError = Some(ce)
          onConsistencyError
          ce
        }
      }
    }
  }

  private def isMessageSuccessful(response: Message) = response.code >= 200 && response.code < 300 && response.error.isEmpty

  private[consistency] def checkPending() {
    consistencyActor !? CheckPending
  }

  private case class PendingTransaction(timestamp: Timestamp, token: Long, maxTimestamp: Timestamp, addedTime: Long,
                                        var completed: Boolean = false) {
    /**
     * Returns true if this transaction has a response and the consistency delay has elapsed since appended.
     */
    def isReady = completed && currentTime - addedTime > consistencyDelay

    def isExpired = currentTime - addedTime > consistencyTimeout
  }

  private case class RequestAppended(request: Request, maxTimestamp: Timestamp)

  private case class ResponseAppended(response: Response)

  private object CheckPending

  private class ConsistencyActor extends Actor with Logging {

    private object Commit

    private object Kill

    private var pendingTransactions: TreeMap[Timestamp, PendingTransaction] = TreeMap()

    @volatile
    var consistentTimestamp: Option[Timestamp] = txLog.getLastLoggedRecord match {
      case Some(record) => record.consistentTimestamp
      case None => None
    }

    val commitScheduler = if (commitFrequency > 0) {
      Some(new Scheduler(this, Commit, Random.nextInt(commitFrequency), commitFrequency,
        blockingMessage = false, autoStart = false, name = Some("ConsistencyActor.Commit")))
    } else {
      None
    }
    val checkPendingScheduler = new Scheduler(this, CheckPending, 100, 100,
      blockingMessage = false, autoStart = false, name = Some("ConsistencyActor.CheckPending"))

    def queueSize = mailboxSize

    def pendingSize = pendingTransactions.size

    override def start() = {
      super.start()
      commitScheduler.foreach(_.start())
      checkPendingScheduler.start()
      this
    }

    def stop() {
      commitScheduler.foreach(_.cancel())
      checkPendingScheduler.cancel()
      this !? Kill
    }

    /**
     * Find the first pending transaction that is consistent i.e. is ready and that had no
     * transactions with a greater timestamp appended before.
     */
    def firstConsistentTransaction: Option[PendingTransaction] = {

      @tailrec
      def next(iterator: Iterator[PendingTransaction], maxTimestamp: Option[Timestamp]): Option[PendingTransaction] = {
        if (iterator.hasNext) {
          val tx = iterator.next()
          val prevMax = maxTimestamp.getOrElse(tx.maxTimestamp)
          if (tx.timestamp > prevMax) {
            None
          } else {
            val txMax = if (prevMax > tx.maxTimestamp) prevMax else tx.maxTimestamp
            if (tx.timestamp == txMax) {
              Some(tx)
            } else {
              next(iterator, Some(txMax))
            }
          }
        } else {
          None
        }
      }

      next(pendingTransactions.valuesIterator.takeWhile(_.isReady), None)
    }

    /**
     * Verify pending transaction consistency and update the current consistent timestamp
     */
    @tailrec
    private def checkPendingConsistency() {
      firstConsistentTransaction match {
        case Some(tx) => {
          // Found a consistent transaction. Use its timestamp as current consistent timestamp and remove all pending
          // transactions up to that transaction
          pendingTransactions = pendingTransactions.from(Timestamp(tx.timestamp.value + 1))
          consistentTimestamp match {
            case Some(ts) if ts > tx.timestamp => {
              // Ensure we are not going back in time!!!
              consistencyErrorCheckPending.mark()
              error("Consistency error consistent timestamp going backward {}.", tx)
              handleConsistencyError()
            }
            case _ =>
          }
          consistentTimestamp = Some(tx.timestamp)

          if (pendingTransactions.isEmpty) {
            // No more pending transactions, append an idle Index. This ensure that the last transaction can be
            // seen by processes that are limited by the recorder currentConsistentTimestamp (e.g. percolation on
            // feeder, store internal GC, etc) if no new write transactions are seen for a while.
            appendIdleIndex(tx.timestamp)
          } else {
            // Check for more consistent transaction
            checkPendingConsistency()
          }
        }
        case None => {
          // Verify if pending head transaction has expired
          pendingTransactions.headOption match {
            case Some((timestamp, tx)) if tx.isExpired => {
              // Pending head transaction has expired before receiving a response, something is very wrong
              pendingTransactions -= timestamp

              consistencyErrorTimeout.mark()
              error("Consistency error timeout on transaction {}.", tx)
              handleConsistencyError()
            }
            case _ => // No pending transactions to check
          }
        }
      }
    }

    def act() {
      loop {
        react {
          case RequestAppended(request, maxTimestamp) => {
            try {
              pendingTransactions.get(request.timestamp) match {
                case Some(tx) => {
                  // Duplicate request
                  consistencyErrorDuplicate.mark()
                  error("Request {} is a duplicate of pending transaction {}. ", request, tx)
                  handleConsistencyError()
                }
                case None => {
                  pendingTransactions += (request.timestamp ->
                    PendingTransaction(request.timestamp, request.token, maxTimestamp, currentTime))
                }
              }
            } catch {
              case e: Exception => {
                consistencyErrorRequest.mark()
                error("Error processing request {}. ", request, e)
                handleConsistencyError(Some(e))
              }
            }
          }
          case ResponseAppended(response) => {
            try {
              pendingTransactions.get(response.timestamp) match {
                case Some(tx) => {
                  // The transaction is complete
                  tx.completed = true
                }
                case None if response.isSuccess => {
                  // The response is successful but does not match a pending transaction.
                  consistencyErrorUnexpectedResponse.mark()
                  error("Response is sucessful but not pending {}. ", response)
                  handleConsistencyError()
                }
                case _ => {
                  unexpectedFailResponse.mark()
                  debug("Received a response message without matching request message: {}", response)
                }
              }

              checkPendingConsistency()
            } catch {
              case e: Exception => {
                consistencyErrorResponse.mark()
                error("Error processing response message {}. ", response, e)
                handleConsistencyError(Some(e))
              }
            }
          }
          case Commit => {
            try {
              debug("Commit transaction log: {}", txLog)
              txLog.commit()
            } catch {
              case e: Exception => {
                consistencyErrorCommit.mark()
                error("Consistency error commiting transaction log of member {}.", member, e)
                handleConsistencyError(Some(e))
              }
            } finally {
              reply(true)
            }
          }
          case CheckPending => {
            try {
              checkPendingConsistency()
            } catch {
              case e: Exception => {
                consistencyErrorCheckPending.mark()
                error("Consistency error checking pending transactions {}.", member, e)
                handleConsistencyError(Some(e))
              }
            } finally {
              reply(true)
            }
          }
          case Kill => {
            try {
              txLog.synchronized {
                txLog.commit()
                txLog.close()
              }
              exit()
            } catch {
              case e: Exception => {
                killError.mark()
                warn("Error killing recorder {}.", member, e)
              }
            } finally {
              reply(true)
            }
          }
        }
      }
    }
  }

}
