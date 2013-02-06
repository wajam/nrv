package com.wajam.nrv.consistency

import com.wajam.nrv.service._
import com.wajam.nrv.data.{MessageType, InMessage}
import com.wajam.nrv.utils.timestamp.{Timestamp, TimestampGenerator}
import java.util.concurrent.atomic.AtomicReference
import com.yammer.metrics.scala.Instrumented
import com.wajam.nrv.utils.Event
import com.wajam.nrv.service.StatusTransitionEvent

/**
 * Consistency that (will eventually) sends read/write to a master replica and replicate modification to the
 * other replicas.
 *
 * IMPORTANT NOTES:
 * - Extends ConsistencyOne until real master/slave consistency is implemented.
 * - Support binding to a single service. The service must extends ConsistentStore.
 */
class ConsistencyMasterSlave(val timestampGenerator: TimestampGenerator, txLogDir: String, txLogEnabled: Boolean)
  extends ConsistencyOne with Instrumented {

  lazy private val recorderNotFound = metrics.meter("recorder-notfound", "recorder-notfound")
  lazy private val recorderAppendError = metrics.meter("recorder-append-error", "recorder-append-error")
  lazy private val unexpectedResponse = metrics.meter("unexpected-response", "unexpected-response")
  lazy private val skipErrorResponse = metrics.meter("skip-error-response", "skip-error-response")

  private val recordersCount = metrics.gauge("recorders-count") {
    recorders.size
  }
  private val recordersAppendQueueSize = metrics.gauge("recorders-append-queue-size") {
    recorders.values.foldLeft[Int](0)((sum, recorder) => {
      sum + recorder.appendQueueSize
    })
  }

  private val lastWriteTimestamp = new AtomicReference[Option[Timestamp]](None)
  @volatile
  private var recorders: Map[ServiceMember, TransactionRecorder] = Map()

  def service = bindedServices.head

  def store = service.asInstanceOf[ConsistentStore]

  override def bindService(service: Service) {
    require(service.isInstanceOf[ConsistentStore],
      "Consistent service must be type of %s but is %s".format(classOf[ConsistentStore], service.getClass))
    require(bindedServices.size == 0, "Cannot bind to multiple services. Already bound to %s".format(bindedServices.head))

    super.bindService(service)
  }

  override def serviceEvent(event: Event) {
    super.serviceEvent(event)

    if (txLogEnabled) {
      event match {
        case event: StatusTransitionEvent if cluster.isLocalNode(event.member.node) => {
          event.to match {
            case MemberStatus.Up => {
              this.synchronized {
                // Iniatialize transaction recorders for local service member going up
                info("Iniatialize transaction recorders for {}", event.member)
                val recorder = new TransactionRecorder(service, event.member, txLogDir)
                recorders = recorders + (event.member -> recorder)
                recorder.start()
              }
            }
            case _ => {
              this.synchronized {
                // Remove transaction recorders for all other cases
                info("Remove transaction recorders for {}", event.member)
                val recorder = recorders.get(event.member)
                recorders = recorders - event.member
                recorder.foreach(_.kill())
              }
            }
          }
        }
        case _ =>
      }
    }
  }

  override def handleIncoming(action: Action, message: InMessage, next: Unit => Unit) {
    if (message.serviceName == service.name && store.requiresConsistency(message)) {
      message.function match {
        case MessageType.FUNCTION_CALL => {
          // Ensure a cluster unique timestamp is present in request
          message.method match {
            case ActionMethod.GET => executeReadRequest(message, next)
            case _ => executeWriteRequest(message, next)
          }
        }
        case MessageType.FUNCTION_RESPONSE => {
          message.method match {
            case ActionMethod.GET => executeReadResponse(message, next)
            case _ => executeWriteResponse(message, next)
          }
        }
      }
    }
    else {
      next()
    }
  }

  private def executeReadRequest(req: InMessage, next: Unit => Unit) {
    lastWriteTimestamp.get() match {
      case Some(timestamp) => {
        // TODO: StringMigration: Remove Any usages
        req.metadata += ("timestamp" -> Seq(timestamp.toString))
        req.metadataOld += ("timestamp" -> timestamp.toString)
        next()
      }
      case None => {
        fetchTimestampAndExecuteNext(req, next)
      }
    }
  }

  private def executeWriteRequest(req: InMessage, next: Unit => Unit) {
    fetchTimestampAndExecuteNext(req, next)
  }

  private def fetchTimestampAndExecuteNext(req: InMessage, next: Unit => Unit) {
    timestampGenerator.fetchTimestamps(req.serviceName, (timestamps: Seq[Timestamp], optException) => {
      try {
        if (optException.isDefined) {
          info("Exception while fetching timestamps.", optException.get.toString)
          throw optException.get
        }
        val timestamp = timestamps(0)
        updateLastTimestamp(timestamp)
        req.metadata += ("timestamp" -> Seq(timestamp.toString))
        req.metadataOld += ("timestamp" -> timestamp.toString)
        next()
      } catch {
        case e: Exception =>
          req.replyWithError(e)
      }
    }, 1, req.token)
  }

  /**
   * Update last timestamp only if greater than the currently saved timestamp. SCN should not give non increasing
   * timestamps, but concurrent execution could be executed in a different order. Since we need to be certain that
   * the last timestamp is effectively the last fetched timestamp, this method must try to set the value as long as
   * either the new value was atomically set or is obsolete.
   */
  private def updateLastTimestamp(timestamp: Timestamp) {
    var updateSuccessful = false
    do {
      val savedTimestamp = lastWriteTimestamp.get()
      updateSuccessful = savedTimestamp match {
        case Some(prevTimestamp) => if (timestamp > prevTimestamp) {
          lastWriteTimestamp.compareAndSet(savedTimestamp, Some(timestamp))
        } else {
          true //the timestamp is less than the last timestamp so our value is obsolete
        }
        case None => lastWriteTimestamp.compareAndSet(savedTimestamp, Some(timestamp))
      }
    } while (!updateSuccessful)
  }

  private def executeReadResponse(res: InMessage, next: Unit => Unit) {
    next()
  }

  private def executeWriteResponse(res: InMessage, next: Unit => Unit) {
    if (txLogEnabled) {
      if (res.code >= 200 && res.code < 300) {
        // Record sucessful request into the transaction log
        res.matchingOutMessage match {
          case Some(req) =>
            try {
              val recorder = service.resolveMembers(req.token, service.resolver.replica).find(
                member => cluster.isLocalNode(member.node)).flatMap(recorders.get(_))
              recorder match {
                case Some(rec) => {
                  debug("Appending request message to transaction log recorder (request={}).", req)
                  rec.append(req)
                }
                case _ => {
                  recorderNotFound.mark()
                  info("No transaction log recorder found for token {} (request={}).", req.token, req)
                }
              }
            } catch {
              case e: Exception => {
                recorderAppendError.mark()
                info("Error appending request message {} to transaction log. ", req, e)
              }
            }
          case None => {
            unexpectedResponse.mark()
            debug("Received a response message without matching request message: {}", res)
          }
        }
      } else {
        skipErrorResponse.mark()
        debug("Received a non successful response message. Skip transaction log: {}", res)
      }
    }

    next()
  }
}
