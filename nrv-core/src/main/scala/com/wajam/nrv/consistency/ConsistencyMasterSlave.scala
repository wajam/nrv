package com.wajam.nrv.consistency

import com.wajam.nrv.service._
import com.wajam.nrv.data.{MessageType, InMessage}
import com.wajam.nrv.utils.timestamp.{Timestamp, TimestampGenerator}
import java.util.concurrent.atomic.AtomicReference
import com.yammer.metrics.scala.Instrumented
import com.wajam.nrv.utils.Event
import com.wajam.nrv.service.StatusTransitionEvent
import persistence.TransactionEvent
import scala.Some

/**
 * Consistency that (will eventually) sends read/write to a master replica and replicate modification to the
 * other replicas.
 *
 * IMPORTANT NOTES:
 *   - Extends ConsistencyOne until real master/slave consistency is implemented.
 *   - Support binding to a single service
 */
class ConsistencyMasterSlave(val timestampGenerator: TimestampGenerator, logDir: String = "") extends ConsistencyOne with Instrumented {

  private val recordersCount = metrics.gauge("recorders-count") {
    recorders.size
  }
  private val recordersAppendQueueSize = metrics.gauge("recorders-append-queue-size") {
    recorders.values.flatten.foldLeft[Int](0)((sum, recorder) => {
      sum + recorder.appendQueueSize
    })
  }

  private val lastWriteTimestamp = new AtomicReference[Option[Timestamp]](None)
  private var recorders: Map[ServiceMember, Seq[TransactionRecorder]] = Map()

  def service = bindedServices.head

  override def bindService(service: Service) {
    if (bindedServices.size == 0) {
      super.bindService(service)
    } else {
      throw new IllegalStateException("Cannot bind to multiple services. Already bound to %s".format(bindedServices.head))
    }
  }

  override def serviceEvent(event: Event) {
    super.serviceEvent(event)

    event match {
      case event: StatusTransitionEvent =>
        event.to match {
          case MemberStatus.Up if cluster.isLocalNode(event.member.node) => {
            // Iniatialize transaction for local service member going up
            val ranges = service.getMemberTokenRanges(event.member)
            recorders = recorders + (event.member -> ranges.map(new TransactionRecorder(service, _, logDir)))
          }
          case _ => {
            // Remove transaction recorder for all other cases
            recorders = recorders - event.member
          }
        }
      case _ =>
    }
  }

  override def handleIncoming(action: Action, message: InMessage, next: Unit => Unit) {

    if (bindedServices.exists(_.name == message.serviceName)) {
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
      // Non binded service message, no special handling
      next()
    }
  }

  private def executeReadRequest(req: InMessage, next: Unit => Unit) {
    lastWriteTimestamp.get() match {
      case Some(timestamp) => {
        req.metadata += ("timestamp" -> timestamp)
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
        req.metadata += ("timestamp" -> timestamp)
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

    if (res.code >= 200 && res.code < 300) {
      // Record sucessful request into the transaction log
      res.matchingOutMessage match {
        case Some(req) =>
          try {
            val recorder: Option[TransactionRecorder] = service.getMemberAtToken(req.token).flatMap(member => {
              recorders.get(member).flatMap(_.find(_.range.contains(req.token)))
            })
            recorder match {
              case Some(rec) => {
                rec.append(req)
              }
              case _ =>
              // TODO: meter + info log no recorder
            }
          } catch {
            case e: Exception => {
              // TODO: meter + error log
            }
          }
        case None => {
          // TODO: meter + info log unexpected response
        }
      }
    } else {
      // TODO: meter
    }

    next()
  }
}
