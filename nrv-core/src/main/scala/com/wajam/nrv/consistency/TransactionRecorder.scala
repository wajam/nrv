package com.wajam.nrv.consistency

import actors.Actor
import com.yammer.metrics.scala.Instrumented
import persistence.LogRecord.{Request, Response, Index}
import persistence.TransactionLog
import com.wajam.nrv.data.{MessageType, Message}
import com.wajam.nrv.utils.timestamp.Timestamp
import com.wajam.nrv.utils.{IdGenerator, TimestampIdGenerator, CurrentTime, Scheduler}
import util.Random
import com.wajam.nrv.Logging
import collection.immutable.TreeMap
import annotation.tailrec

class TransactionRecorder(val member: ResolvedServiceMember, txLog: TransactionLog,
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

  private val consistencyActor = new ConsistencyActor
  private var currentMaxTimestamp: Timestamp = Long.MinValue
  private var consistencyError: Option[ConsistencyException] = None

  def queueSize = consistencyActor.queueSize

  def pendingSize = consistencyActor.pendingSize

  def currentConsistentTimestamp = consistencyActor.consistentTimestamp

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
        consistencyError match {
          case Some(e) => {
            // Do not append request if recorder is inconsistent
            val ce = new ConsistencyException
            ce.initCause(e)
            throw ce
          }
          case None => {
            val request = Request(idGenerator.nextId, currentConsistentTimestamp, message)
            requestMaxTimestamp = if (currentMaxTimestamp > request.timestamp) currentMaxTimestamp else request.timestamp
            currentMaxTimestamp = requestMaxTimestamp
            request
          }
        }
      }
      consistencyActor ! RequestAppended(request, requestMaxTimestamp)
    } catch {
      case e: Exception => {
        consistencyErrorAppend.mark()
        warn("Error appending request message {}. {}", message, e)
        handleConsistencyError(Some(e))

        val ce = new ConsistencyException
        ce.initCause(e)
        throw ce
      }
    }
  }

  private def appendResponse(message: Message) {
    try {
      Consistency.getMessageTimestamp(message) match {
        case Some(timestamp) => {
          // The id generation is synchronized inside the append method implementation
          val response = txLog.append {
            consistencyError match {
              case Some(e) => {
                // Do not append response if recorder is inconsistent
                val ce = new ConsistencyException
                ce.initCause(e)
                throw ce
              }
              case None => {
                Response(idGenerator.nextId, currentConsistentTimestamp, message)
              }
            }
          }
          consistencyActor ! ResponseAppended(response)
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
        val exception = handleConsistencyError(Some(e))

        val ce = new ConsistencyException
        ce.initCause(e)
        throw ce
      }
    }
  }

  private def appendIndex(consistentTimestamp: Timestamp) {
    txLog.append {
      // The id generation is synchronized inside the append method implementation
      consistencyError match {
        case Some(e) => {
          // Do not append index if recorder is inconsistent
          val ce = new ConsistencyException
          ce.initCause(e)
          throw ce
        }
        case None => {
          Index(idGenerator.nextId, Some(consistentTimestamp))
        }
      }
    }
    txLog.commit()
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

    val commitScheduler = new Scheduler(this, Commit, if (commitFrequency > 0) Random.nextInt(commitFrequency) else 0,
      commitFrequency, blockingMessage = true, autoStart = false)
    val checkPendingScheduler = new Scheduler(this, CheckPending, 100, 100, blockingMessage = true, autoStart = false)

    def queueSize = mailboxSize

    def pendingSize = pendingTransactions.size

    override def start() = {
      super.start()
      if (commitFrequency > 0) {
        commitScheduler.start()
      }
      checkPendingScheduler.start()
      this
    }

    def stop() {
      commitScheduler.cancel()
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
            case Some(ts) if (ts > tx.timestamp) => {
              // Ensure we are not going back in time!!!
              consistencyErrorCheckPending.mark()
              error("Consistency error consistent timestamp going backward {}.", tx)
              handleConsistencyError()
            }
            case _ => {
              // No more pending transactions. Append an index to the log before updating the consistentTimestamp to
              // prevent replication source iterator (which read up to consistentTimestamp) to reach the end of the log
              // when the consistentTimestamp is updated.
              if (pendingTransactions.isEmpty) {
                appendIndex(tx.timestamp)
              }
              consistentTimestamp = Some(tx.timestamp)
            }
          }

          // Check for more consistent transaction
          if (!pendingTransactions.isEmpty) {
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
