package com.wajam.nrv.consistency

import actors.Actor
import com.yammer.metrics.scala.Instrumented
import com.wajam.nrv.service.{ServiceMember, Service}
import persistence.LogRecord.Index
import persistence.{TransactionLog, LogRecord}
import com.wajam.nrv.data.{MessageType, Message}
import com.wajam.nrv.utils.timestamp.Timestamp
import com.wajam.nrv.utils.{TimestampIdGenerator, CurrentTime, Scheduler}
import util.Random
import com.wajam.nrv.Logging
import collection.immutable.TreeMap
import annotation.tailrec

class TransactionRecorder(val service: Service, val member: ServiceMember, txLog: TransactionLog, consistencyDelay: Long)
  extends CurrentTime with Instrumented with Logging {

  lazy private val consistencyError = metrics.meter("consistency-error", "consistency-error")
  lazy private val consistencyErrorDuplicate = metrics.meter("consistency-error-duplicate", "consistency-error-duplicate")
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
  private val responseTimeout = math.max(service.responseTimeout + 2000, 15000)
  private val idGenerator = new TimestampIdGenerator

  def queueSize = consistencyActor.queueSize

  def pendingSize = consistencyActor.pendingSize

  def start() {
    consistencyActor.start()
  }

  def kill() {
    consistencyActor.stop()
  }

  def appendMessage(message: Message) {
    txLog.append {
      // No need to explicitly synchronize the id generation as this code is invoked and synchronized inside
      // the append method implementation
      LogRecord(idGenerator.nextId, consistencyActor.consistentTimestamp, message)
    }

    message.function match {
      case MessageType.FUNCTION_CALL => {
        consistencyActor ! HandleRequest(message)
      }
      case MessageType.FUNCTION_RESPONSE => {
        consistencyActor ! HandleResponse(message)
      }
    }
  }

  private def appendIndex() {
    txLog.append {
      // No need to explicitly synchronize the id generation as this code is invoked and synchronized inside
      // the append method implementation
      Index(idGenerator.nextId, consistencyActor.consistentTimestamp)
    }
    txLog.commit()
  }

  private[consistency] def checkPending() {
    consistencyActor !? CheckPending
  }

  private case class PendingTxContext(timestamp: Timestamp, token: Long, addedTime: Long, var completed: Boolean = false) {

    def isConsistent = currentTime - addedTime > consistencyDelay

    def isExpired = currentTime - addedTime > responseTimeout
  }

  private case class HandleRequest(message: Message)

  private case class HandleResponse(message: Message)

  private object CheckPending

  private class ConsistencyActor extends Actor with Logging {

    private object Commit

    private object Kill

    private var pendingTransactions: TreeMap[Timestamp, PendingTxContext] = TreeMap()

    @volatile // TODO: make this cleaner
    var consistentTimestamp: Option[Timestamp] = txLog.getLastLoggedIndex match {
      case Some(index) => index.consistentTimestamp
      case None => None
    }

    // TODO: make commit frequency configurable.
    val commitScheduler = new Scheduler(this, Commit, Random.nextInt(5000), 5000, blockingMessage = true,
      autoStart = false)
    val checkPendingScheduler = new Scheduler(this, CheckPending, 100, 100, blockingMessage = true, autoStart = false)

    def queueSize = mailboxSize

    def pendingSize = pendingTransactions.size

    override def start() = {
      super.start()
      commitScheduler.start()
      checkPendingScheduler.start()
      this
    }

    def stop() {
      commitScheduler.cancel()
      checkPendingScheduler.cancel()
      this !? Kill
    }

    private def isSuccessful(response: Message) = response.code >= 200 && response.code < 300 && response.error.isEmpty

    @tailrec
    private def checkPendingConsistency() {
      pendingTransactions.headOption match {
        case Some((timestamp, context)) if context.isConsistent => {
          // Pending head transaction is ready, remove from pending and update the consistent timestamp
          // TODO: Ensure we are not going back in time!!!
          consistentTimestamp = Some(timestamp)
          pendingTransactions -= timestamp

          if (pendingTransactions.isEmpty) {
            // No more pending transactions, ensure index is appended to the log
            appendIndex()
          } else {
            checkPendingConsistency()
          }
        }
        case Some((timestamp, context)) if context.isExpired => {
          // Pending head transaction has expired before receiving a response, something is very wrong
          consistencyErrorTimeout.mark()
          error("Consistency error timeout on transaction {}.", context)
          raiseConsistencyError()
        }
        case _ => // No pending transactions to check
      }
    }

    private def raiseConsistencyError() {
      consistencyError.mark()

      // TODO: fail the current service member

      // Removes all pending requests to prevent further timeouts. This will generate many unexpected response but this
      // is not really an issue as from this point the service member should go down.
      pendingTransactions = new TreeMap() // TODO: review if this is necessary
    }

    def act() {
      loop {
        react {
          case HandleRequest(message) => {
            try {
              val timestamp = Consistency.getMessageTimestamp(message).get
              pendingTransactions.get(timestamp) match {
                case Some(context) => {
                  // Duplicate message
                  consistencyErrorDuplicate.mark()
                  error("Request message {} is a duplicate of pending context {}. ", message, context)
                  raiseConsistencyError()
                }
                case None => {
                  pendingTransactions += (timestamp -> PendingTxContext(timestamp, message.token, currentTime))
                }
              }
            } catch {
              case e: Exception => {
                consistencyErrorRequest.mark()
                error("Consistency error processing request message {}. ", message, e)
                raiseConsistencyError()
              }
            }
          }
          case HandleResponse(message) => {
            try {
              Consistency.getMessageTimestamp(message) match {
                case Some(timestamp) => {
                  pendingTransactions.get(timestamp) match {
                    case Some(context) => {
                      // The transaction is complete
                      context.completed = true
                    }
                    case None if isSuccessful(message) => {
                      // The response is successful but does not match a pending transaction.
                      consistencyErrorUnexpectedResponse.mark()
                      error("Response is sucessful but not pending {}. ", message)
                      raiseConsistencyError()
                    }
                    case _ => {
                      unexpectedFailResponse.mark()
                      debug("Received a response message without matching request message: {}", message)
                    }
                  }

                  checkPendingConsistency()
                }
                case None if isSuccessful(message) => {
                  consistencyErrorResponseMissingTimestamp.mark()
                  error("Response is sucessful but missing timestamp {}. ", message)
                  raiseConsistencyError()
                }
                case None => {
                  // Ignore failure response without timestamp, they are SCN response error
                  debug("Response failure missing timestamp {}. ", message)
                }
              }
            } catch {
              case e: Exception => {
                consistencyErrorResponse.mark()
                error("Consistency error processing response message {}. ", message, e)
                raiseConsistencyError()
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
                raiseConsistencyError()
              }
            } finally {
              reply(true)
            }
          }
          case CheckPending => {
            try {
              // Verify pending transaction consistency and update consistent timestamp
              checkPendingConsistency()

              pendingTransactions.headOption match {
                case Some((timestamp, context)) if context.isExpired => {
                  consistencyErrorTimeout.mark()
                  error("Consistency error timeout on message {}.", context.timestamp)
                  raiseConsistencyError()
                }
                case _ => // Head transaction not expired, do nothing
              }
            } catch {
              case e: Exception => {
                consistencyErrorCheckPending.mark()
                error("Consistency error checking pending transactions {}.", member, e)
                raiseConsistencyError()
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
