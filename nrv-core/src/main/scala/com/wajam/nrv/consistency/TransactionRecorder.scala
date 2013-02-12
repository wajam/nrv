package com.wajam.nrv.consistency

import actors.Actor
import com.yammer.metrics.scala.Instrumented
import com.wajam.nrv.service.{ServiceMember, Service}
import persistence.{TransactionEvent, FileTransactionLog}
import com.wajam.nrv.data.{MessageType, Message}
import com.wajam.nrv.utils.timestamp.Timestamp
import com.wajam.nrv.utils.{CurrentTime, Scheduler}
import util.Random
import com.wajam.nrv.Logging
import collection.immutable.TreeMap

class TransactionRecorder(val service: Service, val member: ServiceMember, logDir: String, txLogEnabled: Boolean)
  extends CurrentTime with Instrumented with Logging {

  lazy private val consistencyError = metrics.meter("consistency-error", "consistency-error")
  lazy private val consistencyRequestError = metrics.meter("consistency-request-error", "consistency-request-error")
  lazy private val consistencyResponseError = metrics.meter("consistency-response-error", "consistency-response-error")
  lazy private val consistencyAppendError = metrics.meter("consistency-append-error", "consistency-append-error")
  lazy private val consistencyCommitError = metrics.meter("consistency-commit-error", "consistency-commit-error")
  lazy private val consistencyTimeoutError = metrics.meter("consistency-timeout-error", "consistency-timeout-error")
  lazy private val consistencyCheckTimeoutError = metrics.meter("consistency-check-timeout-error", "consistency-check-timeout-error")
  lazy private val unexpectedResponse = metrics.meter("unexpected-response", "unexpected-response")
  lazy private val ignoredFailTransaction = metrics.meter("ignored-fail-tx", "ignored-fail-tx")
  lazy private val killError = metrics.meter("kill-error", "kill-error")

  private var pendingRequests: TreeMap[Timestamp, PendingTxContext] = TreeMap()
  private val txLog: FileTransactionLog = new FileTransactionLog(service.name, member.token, logDir,
    validateTimestamp = true)
  private val responseTimeout = math.max(service.responseTimeout + 1000, 15000)

  val commitScheduler = new Scheduler(RecordingActor, Commit,
    Random.nextInt(5000), 5000, blockingMessage = true, autoStart = false)
  val checkTimeoutScheduler = new Scheduler(RecordingActor, CheckTimeout,
    100, 100, blockingMessage = true, autoStart = false)

  def queueSize = RecordingActor.queueSize

  def pendingSize = pendingRequests.size

  def start() {
    RecordingActor.start()
    commitScheduler.start()
    checkTimeoutScheduler.start()
  }

  def kill() {
    commitScheduler.cancel()
    checkTimeoutScheduler.cancel()
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
      case _ =>
        // TODO: error
    }
  }

  private case class PendingTxContext(request: Message, addedTime: Long, var status: TxStatus = TxStatus.Pending)

  private sealed trait TxStatus

  private object TxStatus {

    object Success extends TxStatus

    object Pending extends TxStatus

  }

  case class HandleRequest(message: Message)

  case class HandleResponse(message: Message)

  object Commit

  object CheckTimeout

  object Kill

  private object RecordingActor extends Actor with Logging {

    import Consistency._

    def queueSize = mailboxSize

    private def isSuccessful(reponse: Message) = reponse.code >= 200 && reponse.code < 300

    private def isExpired(context: PendingTxContext) = currentTime - context.addedTime > responseTimeout

    private def appendCompletedHeadTransaction() {
      pendingRequests.headOption match {
        case Some((timestamp, context)) if context.status == TxStatus.Success => {
          try {
            if (txLogEnabled) {
              txLog.append(TransactionEvent(timestamp, None, context.request.token, context.request))
            }
            pendingRequests -= timestamp
            appendCompletedHeadTransaction()
          } catch {
            case e: Exception => {
              error("Consistency error processing appending message {}. ", context.request, e)
              consistencyAppendError.mark()
              raiseConsistencyError()
            }
          }
        }
        case _ => // First request still pending, do nothing.
      }
    }

    private def raiseConsistencyError() {
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
              pendingRequests += (timestamp -> PendingTxContext(message, currentTime))
            } catch {
              case e: Exception => {
                error("Consistency error processing request message {}. ", message, e)
                consistencyRequestError.mark()
                raiseConsistencyError()
              }
            }
          }
          case HandleResponse(message) => {
            try {
              val timestamp = getMessageTimestamp(message).get
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

                }
                case _ => {
                  unexpectedResponse.mark()
                  debug("Received a response message without matching request message: {}", message)
                }
              }

              // Append the head transaction if completed
              appendCompletedHeadTransaction()
            } catch {
              case e: Exception => {
                error("Consistency error processing response message {}. ", message, e)
                consistencyResponseError.mark()
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
                error("Consistency error commiting transaction log of member {}.", member, e)
                consistencyCommitError.mark()
                raiseConsistencyError()
              }
            } finally {
              reply(sender)
            }
          }
          case CheckTimeout => {
            try {
              pendingRequests.headOption match {
                case Some((timestamp, context)) if isExpired(context) => {
                  consistencyTimeoutError.mark()
                  error("Consistency error timeout on message {}.", context.request)
                  raiseConsistencyError()
                }
                case _ => // Head transaction not expired, do nothing
              }
            } catch {
              case e: Exception => {
                error("Consistency error checking for timeout {}.", member, e)
                consistencyCheckTimeoutError.mark()
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
