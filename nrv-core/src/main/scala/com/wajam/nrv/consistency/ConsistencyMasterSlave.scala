package com.wajam.nrv.consistency

import com.wajam.nrv.service._
import com.wajam.nrv.data.{OutMessage, Message, MessageType, InMessage}
import com.wajam.nrv.utils.timestamp.{Timestamp, TimestampGenerator}
import java.util.concurrent.atomic.AtomicReference
import com.yammer.metrics.scala.Instrumented
import com.wajam.nrv.utils.Event
import com.wajam.nrv.service.StatusTransitionEvent
import persistence.{NullTransactionLog, FileTransactionLog}

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

  import Consistency._

  lazy private val recorderNotFound = metrics.meter("recorder-not-found", "recorder-not-found")
  private val recordersCount = metrics.gauge("recorders-count") {
    recorders.size
  }
  private val recordersQueueSize = metrics.gauge("recorders-queue-size") {
    recorders.values.foldLeft[Int](0)((sum, recorder) => {
      sum + recorder.queueSize
    })
  }
  private val recordersPendingSize = metrics.gauge("recorders-pending-tx-size") {
    recorders.values.foldLeft[Int](0)((sum, recorder) => {
      sum + recorder.pendingSize
    })
  }

  private val lastWriteTimestamp = new AtomicReference[Option[Timestamp]](None)
  @volatile
  private var recorders: Map[ServiceMember, TransactionRecorder] = Map() // updates are synchronized but lookups are not

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

    event match {
      case event: StatusTransitionEvent if cluster.isLocalNode(event.member.node) => {
        event.to match {
          case MemberStatus.Up => {
            this.synchronized {
              // Iniatialize transaction recorder for local service member going up
              info("Iniatialize transaction recorders for {}", event.member)
              val txLog = if (txLogEnabled) {
                new FileTransactionLog(service.name, event.member.token, txLogDir, validateTimestamp = true)
              } else {
                NullTransactionLog
              }
              val recorder = new TransactionRecorder(service, event.member, txLog,
                appendDelay = timestampGenerator.responseTimeout + 1000)
              recorders += (event.member -> recorder)
              recorder.start()
            }
          }
          case _ => {
            this.synchronized {
              // Remove transaction recorder for all other cases
              info("Remove transaction recorders for {}", event.member)
              val recorder = recorders.get(event.member)
              recorders -= event.member
              recorder.foreach(_.kill())
            }
          }
        }
      }
      case _ =>
    }
  }

  private def requiresConsistency(message: Message) = {
    message.serviceName == service.name && store.requiresConsistency(message)
  }

  override def handleIncoming(action: Action, message: InMessage, next: Unit => Unit) {
    message.function match {
      case MessageType.FUNCTION_CALL if requiresConsistency(message) => {
        message.method match {
          case ActionMethod.GET => executeConsistentReadRequest(message, next)
          case _ => executeConsistentWriteRequest(message, next)
        }
      }
      case _ => {
        next()
      }
    }
  }

  private def executeConsistentReadRequest(req: InMessage, next: Unit => Unit) {
    lastWriteTimestamp.get() match {
      case Some(timestamp) => {
        setMessageTimestamp(req, timestamp)
        next()
      }
      case None => {
        fetchTimestampAndExecuteNext(req, next)
      }
    }
  }

  private def executeConsistentWriteRequest(req: InMessage, next: Unit => Unit) {
    fetchTimestampAndExecuteNext(req, _ => {
      recordConsistentMessage(req)
      next()
    })
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
        setMessageTimestamp(req, timestamp)
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

  override def handleOutgoing(action: Action, message: OutMessage, next: Unit => Unit) {
    handleOutgoing(action, message)

    message.function match {
      case MessageType.FUNCTION_RESPONSE if requiresConsistency(message) => {
        message.method match {
          case ActionMethod.GET => executeConsistentReadResponse(message, next)
          case _ => executeConsistentWriteResponse(message, next)
        }
      }
      case _ => {
        next()
      }
    }
  }

  private def executeConsistentReadResponse(res: OutMessage, next: Unit => Unit) {
    next()
  }

  private def executeConsistentWriteResponse(res: OutMessage, next: Unit => Unit) {
    recordConsistentMessage(res)
    next()
  }

  private def recordConsistentMessage(message: Message) {
    val recorderOpt = service.resolveMembers(message.token, service.resolver.replica).find(
      member => cluster.isLocalNode(member.node)).flatMap(recorders.get(_))
    recorderOpt match {
      case Some(recorder) => {
        recorder.handleMessage(message)
      }
      case _ => {
        recorderNotFound.mark()
        debug("No transaction log recorder found for token {} (message={}).", message.token, message)
      }
    }
  }
}
