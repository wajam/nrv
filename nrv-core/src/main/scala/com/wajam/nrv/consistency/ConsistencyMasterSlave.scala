package com.wajam.nrv.consistency

import com.wajam.nrv.service.{ActionMethod, Action}
import com.wajam.nrv.data.{MessageType, InMessage}
import com.wajam.nrv.utils.timestamp.{Timestamp, TimestampGenerator}
import java.util.concurrent.atomic.AtomicReference

/**
 * Consistency that (will eventually) sends read/write to a master replica and replicate modification to the
 * other replicas.
 *
 * TEMPORARY extends ConsistencyOne until real master/slave consistency is implemented.
 */
class ConsistencyMasterSlave(val timestampGenerator: TimestampGenerator) extends ConsistencyOne {

  private val lastWriteTimestamp = new AtomicReference[Option[Timestamp]](None)

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
        case _ => {
          // Response, no special handling
          next()
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
}
