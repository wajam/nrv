package com.wajam.nrv.consistency

import com.wajam.nrv.service._
import com.wajam.nrv.data.{OutMessage, Message, MessageType, InMessage}
import com.wajam.nrv.utils.timestamp.{Timestamp, TimestampGenerator}
import java.util.concurrent.atomic.AtomicReference
import com.yammer.metrics.scala.{Meter, Instrumented}
import com.wajam.nrv.utils.Event
import com.wajam.nrv.service.StatusTransitionEvent
import persistence.{NullTransactionLog, FileTransactionLog}
import java.util.concurrent.TimeUnit
import com.yammer.metrics.core.Gauge
import com.wajam.nrv.UnavailableException

/**
 * Consistency that (will eventually) sends read/write to a master replica and replicate modification to the
 * other replicas.
 *
 * IMPORTANT NOTES:
 * - Extends ConsistencyOne until real master/slave consistency is implemented.
 * - Support binding to a single service. The service must extends ConsistentStore.
 */
class ConsistencyMasterSlave(val timestampGenerator: TimestampGenerator, txLogDir: String, txLogEnabled: Boolean,
                             txLogRolloverSize: Int = 50000000, txLogCommitFrequency: Int = 5000)
  extends ConsistencyOne {

  import Consistency._

  private val lastWriteTimestamp = new AtomicReference[Option[Timestamp]](None)
  @volatile // updates are synchronized but lookups are not
  private var recorders: Map[Long, TransactionRecorder] = Map()

  private var consistencyStates: Map[Long, MemberConsistencyState] = Map()

  private var metrics: Metrics = null

  def service = bindedServices.head

  def store = service.asInstanceOf[ConsistentStore]

  override def bindService(service: Service) {
    require(service.isInstanceOf[ConsistentStore],
      "Consistent service must be type of %s but is %s".format(classOf[ConsistentStore], service.getClass))
    require(bindedServices.size == 0, "Cannot bind to multiple services. Already bound to %s".format(bindedServices.head))

    super.bindService(service)

    metrics = new Metrics(service.name)

    // TODO: cleanup service registration
    // Setup current consistent timestamp lookup storage function
    info("Setup consistent timestamp lookup for service", service.name)
    store.setCurrentConsistentTimestamp((range) => {
      val timestamp = recorders.valuesIterator.collectFirst({
        case recorder if recorder.member.ranges.contains(range) => recorder.currentConsistentTimestamp
      })
      timestamp match {
        case Some(Some(ts)) => ts
        case _ => Long.MinValue
      }
    })
  }

  override def serviceEvent(event: Event) {
    super.serviceEvent(event)

    event match {
      case event: StatusTransitionAttemptEvent if txLogEnabled && event.to == MemberStatus.Up => {
        // Trying to transition the service member up. Ensure service member is consistent before allowing it to go up.
        this.synchronized {
          consistencyStates.get(event.member.token) match {
            case Some(MemberConsistencyState.Ok) => {
              // Service member is consistent, let member status goes up!
              event.vote(pass = true)
            }
            case Some(MemberConsistencyState.Recovering) => {
              // Service member is recovering, stay down until recovery is completed
              event.vote(pass = false)
            }
            case Some(MemberConsistencyState.Error) => {
              // Service member had a consistency error, reset the consistency state and retry new recovery later.
              consistencyStates -= event.member.token
              event.vote(pass = false)
            }
            case None => {
              // No consistency state! Perform consistency validation and restore member consistency if possible.
              val member = ResolvedServiceMember(service, event.member)
              metrics.consistencyRecovering.mark()
              consistencyStates += (member.token -> MemberConsistencyState.Recovering)

              restoreMemberConsistency(member, onSuccess = {
                metrics.consistencyOk.mark()
                this.synchronized {
                  consistencyStates += (member.token -> MemberConsistencyState.Ok)
                }
              }, onError = {
                metrics.consistencyError.mark()
                this.synchronized {
                  consistencyStates += (member.token -> MemberConsistencyState.Error)
                }
              })
              event.vote(pass = false)
            }
          }
        }
      }
      case event: StatusTransitionEvent if cluster.isLocalNode(event.member.node) => {
        event.to match {
          case MemberStatus.Up => {
            this.synchronized {
              // Iniatialize transaction recorder for local service member going up
              info("Iniatialize transaction recorders for {}", event.member)
              val txLog = if (txLogEnabled) {
                new FileTransactionLog(service.name, event.member.token, txLogDir, txLogRolloverSize)
              } else {
                NullTransactionLog
              }
              val recorder = new TransactionRecorder(ResolvedServiceMember(service, event.member), txLog,
                consistencyDelay = timestampGenerator.responseTimeout + 1000,
                consistencyTimeout = math.max(service.responseTimeout + 2000, 15000),
                commitFrequency = txLogCommitFrequency, onConsistencyError = {
                  metrics.consistencyError.mark()
                  this.synchronized {
                    consistencyStates += (event.member.token -> MemberConsistencyState.Error)
                  }
                  event.member.setStatus(MemberStatus.Down, triggerEvent = true)
                })
              recorders += (event.member.token -> recorder)
              recorder.start()
            }
          }
          case _ => {
            this.synchronized {
              // Remove transaction recorder for all other cases
              info("Remove transaction recorders for {}", event.member)
              val recorder = recorders.get(event.member.token)
              recorders -= event.member.token
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

  private def getLocalServiceMemberFromMessage(message: Message): Option[ServiceMember] = {
    service.resolveMembers(message.token, 1).find(member => cluster.isLocalNode(member.node))
  }

  private def isMessageLocalMemberUp(message: Message): Boolean = {
    getLocalServiceMemberFromMessage(message) match {
      case Some(member) if member.status == MemberStatus.Up => true
      case _ => false
    }
  }

  override def handleIncoming(action: Action, message: InMessage, next: Unit => Unit) {
    message.function match {
      case MessageType.FUNCTION_CALL if requiresConsistency(message) => {
        if (isMessageLocalMemberUp(message)) {
          message.method match {
            case ActionMethod.GET => executeConsistentReadRequest(message, next)
            case _ => executeConsistentWriteRequest(message, next)
          }
        } else {
          metrics.blockedInMessageServiceDown.mark()
          message.replyWithError(new UnavailableException)
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
    val recorderOpt = for {
      member <- getLocalServiceMemberFromMessage(message)
      recorder <- recorders.get(member.token)
    } yield recorder
    recorderOpt match {
      case Some(recorder) => {
        recorder.appendMessage(message)
      }
      case _ => {
        metrics.recorderNotFound.mark()
        debug("No transaction log recorder found for token {} (message={}).", message.token, message)
      }
    }
  }

  private def restoreMemberConsistency(member: ResolvedServiceMember, onSuccess: => Unit, onError: => Unit) {
    try {
      val finalLogIndex = new ConsistencyRecovery(txLogDir, store).restoreMemberConsistency(member, onError)
      finalLogIndex.map(_.consistentTimestamp) match {
        case Some(lastLogTimestamp) => {
          // Ensure that transaction log and storage have the same final transaction timestamp
          val lastStoreTimestamp = store.getLastTimestamp(member.ranges)
          if (lastLogTimestamp == lastStoreTimestamp) {
            info("The service member transaction log and store are consistent {}", member)
            onSuccess
          } else {
            error("Transaction log and storage last timestamps are different! (log={}, store={}) {}",
              lastLogTimestamp, lastStoreTimestamp, member)
            onError
          }
        }
        case None => {
          // No transaction log, assume the store is consistent
          info("The service member has no transaction log and is assumed to be consistent {}", member)
          onSuccess
        }
      }
    } catch {
      case e: Exception => {
        error("Got an exception during the service member recovery! {}", member, e)
        onError
      }
    }
  }

  private class Metrics(scope: String) extends Instrumented {
    lazy val recorderNotFound = new Meter(metrics.metricsRegistry.newMeter(classOf[ConsistencyMasterSlave],
      "recorder-not-found", scope, "recorder-not-found", TimeUnit.SECONDS))
    lazy val consistencyOk = new Meter(metrics.metricsRegistry.newMeter(classOf[ConsistencyMasterSlave],
      "consistency-ok", scope, "consistency-ok", TimeUnit.SECONDS))
    lazy val consistencyRecovering = new Meter(metrics.metricsRegistry.newMeter(classOf[ConsistencyMasterSlave],
      "consistency-recovering", scope, "consistency-recovering", TimeUnit.SECONDS))
    lazy val consistencyError = new Meter(metrics.metricsRegistry.newMeter(classOf[ConsistencyMasterSlave],
      "consistency-error", scope, "consistency-error", TimeUnit.SECONDS))
    lazy val blockedInMessageServiceDown = new Meter(metrics.metricsRegistry.newMeter(classOf[ConsistencyMasterSlave],
      "block-in-msg-service-down", scope, "block-in-msg-service-down", TimeUnit.SECONDS))


    private val recordersCount = metrics.metricsRegistry.newGauge(classOf[ConsistencyMasterSlave],
      "recorders-count", scope, new Gauge[Long] {
        def value = {
          recorders.size
        }
      }
    )
    private val recordersQueueSize = metrics.metricsRegistry.newGauge(classOf[ConsistencyMasterSlave],
      "recorders-queue-size", scope, new Gauge[Long] {
        def value = {
          recorders.values.foldLeft[Int](0)((sum, recorder) => sum + recorder.queueSize)
        }
      }
    )
    private val recordersPendingSize = metrics.metricsRegistry.newGauge(classOf[ConsistencyMasterSlave],
      "recorders-pending-tx-size", scope, new Gauge[Long] {
        def value = {
          recorders.values.foldLeft[Int](0)((sum, recorder) => sum + recorder.pendingSize)
        }
      }
    )
  }
}

sealed trait MemberConsistencyState

object MemberConsistencyState {

  object Recovering extends MemberConsistencyState

  object Ok extends MemberConsistencyState

  object Error extends MemberConsistencyState

}
