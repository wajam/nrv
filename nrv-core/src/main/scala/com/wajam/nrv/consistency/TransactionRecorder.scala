package com.wajam.nrv.consistency

import actors.Actor
import com.yammer.metrics.scala.Instrumented
import com.wajam.nrv.service.{ServiceMember, Service}
import persistence.{TransactionLog, TransactionEvent}
import com.wajam.nrv.data.{MessageType, Message}
import com.wajam.nrv.utils.timestamp.Timestamp
import com.wajam.nrv.utils.{CurrentTime, Scheduler}
import util.Random
import com.wajam.nrv.{TimeoutException, Logging}
import collection.immutable.TreeMap

class TransactionRecorder(val service: Service, val member: ServiceMember, txLog: TransactionLog, appendDelay: Long)
  extends CurrentTime with Instrumented with Logging {

  lazy private val consistencyError = metrics.meter("consistency-error", "consistency-error")
  lazy private val consistencyDuplicateError = metrics.meter("consistency-duplicate-error", "consistency-duplicate-error")
  lazy private val consistencyRequestError = metrics.meter("consistency-request-error", "consistency-request-error")
  lazy private val consistencyResponseError = metrics.meter("consistency-response-error", "consistency-response-error")
  lazy private val consistencyAppendError = metrics.meter("consistency-append-error", "consistency-append-error")
  lazy private val consistencyCommitError = metrics.meter("consistency-commit-error", "consistency-commit-error")
  lazy private val consistencyTimeoutError = metrics.meter("consistency-timeout-error", "consistency-timeout-error")
  lazy private val consistencyCheckPendingError = metrics.meter("consistency-check-pending-error", "consistency-check-pending-error")
  lazy private val unexpectedSuccessResponse = metrics.meter("unexpected-success-response", "unexpected-success-response")
  lazy private val unexpectedFailResponse = metrics.meter("unexpected-fail-response", "unexpected-fail-response")
  lazy private val ignoredFailTransaction = metrics.meter("ignored-fail-transaction", "ignored-fail-transaction")
  lazy private val timeoutTransaction = metrics.meter("timeout-transaction", "timeout-transaction")
  lazy private val killError = metrics.meter("kill-error", "kill-error")

  private var pendingRequests: TreeMap[Timestamp, PendingTxContext] = TreeMap()
  private var lastTimestamp = txLog.getLastLoggedTimestamp
  private val responseTimeout = math.max(service.responseTimeout + 1000, 15000)

  val commitScheduler = new Scheduler(RecordingActor, Commit,
    Random.nextInt(5000), 5000, blockingMessage = true, autoStart = false)
  val checkPendingScheduler = new Scheduler(RecordingActor, CheckPending,
    100, 100, blockingMessage = true, autoStart = false)


  def queueSize = RecordingActor.queueSize

  def pendingSize = pendingRequests.size

  def start() {
    RecordingActor.start()
    commitScheduler.start()
    checkPendingScheduler.start()
  }

  def kill() {
    commitScheduler.cancel()
    checkPendingScheduler.cancel()
    RecordingActor ! Kill
  }

  def handleMessage(message: Message) {
    message.function match {
      case MessageType.FUNCTION_CALL => {
        RecordingActor ! HandleRequest(message)
      }
      case MessageType.FUNCTION_RESPONSE => {
        RecordingActor ! HandleResponse(message)
      }
    }
  }

  private[consistency] def checkPending() {
    RecordingActor !? CheckPending
  }

  private case class PendingTxContext(request: Message, addedTime: Long, var status: TxStatus = TxStatus.Pending) {

    def isReady = status == TxStatus.Success && currentTime - addedTime > appendDelay

    def isExpired = currentTime - addedTime > responseTimeout
  }

  private sealed trait TxStatus

  private object TxStatus {

    object Success extends TxStatus

    object Pending extends TxStatus

  }

  private case class HandleRequest(message: Message)

  private case class HandleResponse(message: Message)

  private object Commit

  private object CheckPending

  private object Kill

  private object RecordingActor extends Actor with Logging {

    import Consistency._

    def queueSize = mailboxSize

    private def isSuccessful(response: Message) = response.code >= 200 && response.code < 300 && response.error.isEmpty

    private def isTimeout(response: Message) = {
      response.error match {
        case e: TimeoutException => true
        case _ => false
      }
    }

    private def appendAllReadyTransactions() {
      // Not implemented as tail recursive because of the try/catch
      while (appendHead()) {}
    }

    /**
     * Append the first pending transaction if ready. Returns true if appended or false if not
     */
    private def appendHead(): Boolean = {
      pendingRequests.headOption match {
        case Some((timestamp, context)) if context.isReady => {
          try {
            txLog.append(TransactionEvent(timestamp, lastTimestamp, context.request.token, context.request))
            lastTimestamp = Some(timestamp)
            pendingRequests -= timestamp
            true
          } catch {
            case e: Exception => {
              consistencyAppendError.mark()
              error("Consistency error processing appending message {}. ", context.request, e)
              raiseConsistencyError()
              false
            }
          }
        }
        case _ => {
          // First transaction still pending, do nothing.
          false
        }
      }
    }

    private def raiseConsistencyError(/*errorMeter: Meter, errorMsg: String, errorMsgParams: Any**/) {
      consistencyError.mark()

      // TODO: fail the current service member

      // Removes all pending requests to prevent further timeouts. This will generate many unexpected response but this
      // is not really an issue as from this point the service member should go down.
      pendingRequests = new TreeMap()
    }

    def act() {
      loop {
        react {
          case HandleRequest(message) => {
            try {
              val timestamp = getMessageTimestamp(message).get
              pendingRequests.get(timestamp) match {
                case Some(context) => {
                  // Duplicate message
                  consistencyDuplicateError.mark()
                  error("Request message {} is a duplicate of pending context {}. ", message, context)
                  raiseConsistencyError()
                }
                case None => {
                  pendingRequests += (timestamp -> PendingTxContext(message, currentTime))
                }
              }
            } catch {
              case e: Exception => {
                consistencyRequestError.mark()
                error("Consistency error processing request message {}. ", message, e)
                raiseConsistencyError()
              }
            }
          }
          case HandleResponse(message) => {
            try {
              getMessageTimestamp(message) match {
                case Some(timestamp) => {
                  pendingRequests.get(timestamp) match {
                    case Some(context) if isSuccessful(message) => {
                      // The transaction is complete and successful!
                      context.status = TxStatus.Success
                    }
                    case Some(context) => {
                      // The transaction is complete but the response is unsucessful, ignore the transaction
                      ignoredFailTransaction.mark()
                      pendingRequests -= timestamp
                      debug("Received a non successful response message. Do not append transaction to log: {}", message)

                      // If the transaction has timed out. Ensure transaction is not in storage
                      if (isTimeout(message)) {
                        timeoutTransaction.mark()
                        // TODO: Truncate timestamp in storage
                      }
                    }
                    case None if isSuccessful(message) => {
                      // The response is successful but does not match a pending request. Ensure transaction not in storage.
                      unexpectedSuccessResponse.mark()
                      // TODO: Truncate timestamp in storage
                    }
                    case _ => {
                      unexpectedFailResponse.mark()
                      debug("Received a response message without matching request message: {}", message)
                    }
                  }

                  // Append all completed head transactions
                  appendAllReadyTransactions()
                }
                case None if isSuccessful(message) => {
                  consistencyResponseError.mark()
                  error("Response is sucessful but missing timestamp {}. ", message)
                  raiseConsistencyError()
                }
                case _ => {
                  // Just ignore failure response without timestamp
                  debug("Response failure missing timestamp {}. ", message)
                }
              }
            } catch {
              case e: Exception => {
                consistencyResponseError.mark()
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
                consistencyCommitError.mark()
                error("Consistency error commiting transaction log of member {}.", member, e)
                raiseConsistencyError()
              }
            } finally {
              reply(true)
            }
          }
          case CheckPending => {
            try {
              // Append all transactions completed and ready
              appendAllReadyTransactions()

              pendingRequests.headOption match {
                case Some((timestamp, context)) if context.isExpired => {
                  consistencyTimeoutError.mark()
                  error("Consistency error timeout on message {}.", context.request)
                  raiseConsistencyError()
                }
                case _ => // Head transaction not expired, do nothing
              }
            } catch {
              case e: Exception => {
                consistencyCheckPendingError.mark()
                error("Consistency error checking pending transactions {}.", member, e)
                raiseConsistencyError()
              }
            } finally {
              reply(sender)
            }
          }
          case Kill => {
            try {
              txLog.commit()
              txLog.close()
              exit()
            } catch {
              case e: Exception => {
                killError.mark()
                warn("Error killing recorder {}.", member, e)
              }
            }
          }
        }
      }
    }
  }

}
