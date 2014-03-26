package com.wajam.nrv.consistency.replication

import com.wajam.nrv.service._
import com.wajam.nrv.data._
import com.wajam.nrv.consistency.{ConsistentStore, ResolvedServiceMember}
import com.wajam.nrv.consistency.log.TransactionLog
import com.wajam.nrv.cluster.Node
import scala.actors.Actor
import collection.immutable.TreeMap
import ReplicationAPIParams._
import com.wajam.commons.Logging
import com.yammer.metrics.scala.Instrumented
import com.wajam.commons.{CurrentTime, UuidStringGenerator, Observable, Event}
import com.wajam.nrv.utils.Scheduler
import com.wajam.nrv.utils.timestamp.Timestamp
import com.wajam.nrv.consistency.replication.MasterReplicationSessionManager.ReplicationLagChanged

/**
 * Manage all the local master replication sessions the local service is acting as a replication source. The replication
 * source must be the master replica and the replication source must have transaction logs enabled.
 */
class MasterReplicationSessionManager(service: Service, store: ConsistentStore,
                           getTransactionLog: (ResolvedServiceMember) => TransactionLog,
                           getMemberCurrentConsistentTimestamp: (ResolvedServiceMember) => Option[Timestamp],
                           pushAction: Action, pushTps: Int, pushWindowSize: => Int, maxIdleDurationInMs: Long)
  extends CurrentTime with UuidStringGenerator with Observable with Logging with Instrumented {

  private val manager = new SessionManagerActor

  private val serviceScope = service.name.replace(".", "-")
  private val sessionsGauge = metrics.gauge("sessions", serviceScope) {
    manager.sessionsCount
  }

  def start() {
    manager.start()
  }

  def stop() {
    manager !? SessionManagerProtocol.Kill
  }

  def terminateMemberSessions(member: ResolvedServiceMember) {
    manager ! SessionManagerProtocol.TerminateMemberSessions(member)
  }

  def handleOpenSessionMessage(message: InMessage) {
    manager ! SessionManagerProtocol.OpenSessionRequest(message)
  }

  def handleCloseSessionMessage(message: InMessage) {
    manager ! SessionManagerProtocol.CloseSessionRequest(message)
  }

  def sessions: Iterable[ReplicationSession] = {
    val allSessions = manager !? SessionManagerProtocol.GetSessions
    allSessions.asInstanceOf[Iterable[ReplicationSession]]
  }

  object SessionManagerProtocol {

    case class OpenSessionRequest(message: InMessage)

    case class CloseSessionRequest(message: InMessage)

    object GetSessions

    case class TerminateMemberSessions(member: ResolvedServiceMember)

    case class TerminateSession(sessionActor: SessionActor, error: Option[Exception] = None)

    object Kill

  }

  class SessionManagerActor extends Actor {

    private lazy val openSessionMeter = metrics.meter("open-session", "open-session", serviceScope)
    private lazy val openSessionErrorMeter = metrics.meter("open-session-error", "open-session-error", serviceScope)

    private lazy val closeSessionMeter = metrics.meter("close-session", "close-session", serviceScope)
    private lazy val closeSessionErrorMeter = metrics.meter("close-session-error", "close-session-error", serviceScope)

    private lazy val terminateSessionErrorMeter = metrics.meter("terminate-session-error", "terminate-session-error", serviceScope)

    import SessionManagerProtocol._

    private var sessions: List[SessionActor] = Nil

    def sessionsCount = sessions.size

    private def createResolvedServiceMember(token: Long): ResolvedServiceMember = {
      service.getMemberAtToken(token) match {
        case Some(member) if service.cluster.isLocalNode(member.node) => {
          ResolvedServiceMember(service.name, token, service.getMemberTokenRanges(member))
        }
        case Some(member) => {
          throw new Exception("local node not master of token '%d' service member (master=%s)".format(token, member.node))
        }
        case None => {
          throw new Exception("token '%d' service member not found".format(token))
        }
      }
    }

    private def createSourceIterator(resolvedMember: ResolvedServiceMember, startTimestamp: Timestamp,
                                     isLiveReplication: Boolean): ReplicationSourceIterator = {
      val txLog = getTransactionLog(resolvedMember)
      val sourceIterator = txLog.firstRecord(None) match {
        case Some(logRecord) if isLiveReplication && logRecord.timestamp <= startTimestamp => {
          // Live replication mode. Use the transaction log if the first transaction log record is before the starting
          // timestamp.
          info("Using TransactionLogReplicationIterator. start={}, end={}, member={}", startTimestamp,
            getMemberCurrentConsistentTimestamp(resolvedMember), resolvedMember)

          val member = service.getMemberAtToken(resolvedMember.token)
          def mustDrainMember = member.exists(_.status == MemberStatus.Leaving)

          new TransactionLogReplicationIterator(resolvedMember, startTimestamp, txLog,
            getMemberCurrentConsistentTimestamp(resolvedMember), mustDrainMember)
        }
        case Some(_) => {
          // Not in live replication mode or the first transaction log record is after the replication starting
          // timestamp or store source is forced. Use the consistent store as the replication source up to the
          // current member consistent timestamp.
          val endTimestamp = getMemberCurrentConsistentTimestamp(resolvedMember).get
          info("Using ConsistentStoreReplicationIterator. start={}, end={}, member={}",
            startTimestamp, endTimestamp, resolvedMember)
          new ConsistentStoreReplicationIterator(resolvedMember, startTimestamp, endTimestamp, store)
        }
        case None => {
          // There are no transaction log!!! Cannot replicate if transaction log is not enabled.
          throw new Exception("No transaction log! Cannot replicate without transaction log")
        }
      }
      // Exclude startTimestamp
      sourceIterator.withFilter {
        case Some(msg) => msg.timestamp.get > startTimestamp
        case None => true
      }
    }

    def act() {
      loop {
        react {
          case OpenSessionRequest(message) => {
            try {
              // TODO: limit the number of concurrent replication sessions? Global limit or per service member?
              openSessionMeter.mark()
              debug("Received a open session request {}", message)

              implicit val request = message
              val start = getOptionalParamLongValue(Start).getOrElse(Long.MinValue)
              val token = getParamLongValue(Token)
              val cookie = getOptionalParamStringValue(Cookie)
              val isLiveReplication = getOptionalParamStringValue(Mode).map(ReplicationMode(_)) match {
                case Some(ReplicationMode.Live) => true
                case _ => false
              }
              val member = createResolvedServiceMember(token)
              val source = createSourceIterator(member, start, isLiveReplication)

              val session = new SessionActor(nextId, member, source, cookie.getOrElse(""), message.source, getOptionalParamLongValue(Start).map(Timestamp(_)))
              sessions = session :: sessions

              // Reply with an open session response
              val response = new OutMessage()
              response.parameters += (SessionId -> session.sessionId)
              response.parameters += (Start -> start)
              getMemberCurrentConsistentTimestamp(member).foreach(ts => response.parameters += (ConsistentTimestamp -> ts.value))
              source.end.foreach(ts => response.parameters += (End -> ts.value))
              cookie.foreach(value => response.parameters += (Cookie -> value))
              message.reply(response)

              session.start()
            } catch {
              case e: Exception => {
                openSessionErrorMeter.mark()
                warn("Error processing open session request {}: ", message, e)
                try {
                  val response = new OutMessage()
                  response.error = Some(e)
                  message.reply(response)
                } catch {
                  case ex: Exception => warn("Error replying open session request error {}: ", message, ex)
                }
              }
            }
          }
          case CloseSessionRequest(message) => {
            try {
              closeSessionMeter.mark()
              debug("Received a close session request {}", message)

              val id = getSessionId(message)
              sessions.find(_.sessionId == id).foreach(session => {
                session !? SessionProtocol.Kill
                sessions = sessions.filterNot(_ == session)
              })
              message.reply(Nil)
            } catch {
              case e: Exception => {
                closeSessionErrorMeter.mark()
                warn("Error processing close session request {}: ", message, e)
              }
            }
          }
          case GetSessions => {
            try {
              val allSessions = sessions.map(sessionActor => {
                val mode = if (sessionActor.source.end.isDefined) ReplicationMode.Bootstrap else ReplicationMode.Live
                ReplicationSession(sessionActor.member, sessionActor.cookie, mode, sessionActor.slave,
                  Some(sessionActor.sessionId), Some(sessionActor.source.start), sessionActor.source.end, sessionActor.replicationLagSeconds)
              })
              reply(allSessions)
            } catch {
              case e: Exception => {
                warn("Error processing get sessions {}", e)
                reply(Nil)
              }
            }
          }
          case TerminateSession(sessionActor, exception) => {
            try {
              sessions.find(_ == sessionActor).foreach(session => {
                info("Session actor {} wants to be terminated. Dutifully perform euthanasia! {}",
                  sessionActor.sessionId, sessionActor.member)
                session !? SessionProtocol.Kill
                sessions = sessions.filterNot(_ == session)
              })
            } catch {
              case e: Exception => {
                terminateSessionErrorMeter.mark()
                warn("Error processing terminate session {}: ", sessionActor.member, e)
              }
            }
          }
          case TerminateMemberSessions(member) => {
            try {
              sessions.withFilter(_.member == member).foreach(session => {
                info("Session {} is terminated. {}",
                  session.sessionId, member)
                session !? SessionProtocol.Kill
                sessions = sessions.filterNot(_ == session)
              })
            } catch {
              case e: Exception => {
                terminateSessionErrorMeter.mark()
                warn("Error processing terminate all sessions {}: ", e)
              }
            }
          }
          case Kill => {
            try {
              sessions.foreach(_ !? SessionProtocol.Kill)
              sessions = Nil
            } catch {
              case e: Exception => {
                warn("Error killing session manager ({}). {}", service.name, e)
              }
            } finally {
              reply(true)
            }
          }
        }
      }
    }

  }

  object SessionProtocol {

    object PushNext

    case class Ack(sequence: Long)

    object Kill

  }

  class SessionActor(val sessionId: String,
                     val member: ResolvedServiceMember,
                     val source: ReplicationSourceIterator,
                     val cookie: String,
                     val slave: Node,
                     startTimestamp: Option[Timestamp])
    extends Actor with Logging {

    private lazy val pushMeter = metrics.meter("push", "push", member.scopeName)
    private lazy val pushErrorMeter = metrics.meter("push-error", "push-error", member.scopeName)

    private lazy val ackMeter = metrics.meter("ack", "ack", member.scopeName)
    private lazy val ackTimeoutMeter = metrics.meter("ack-timeout", "ack-timeout", member.scopeName)
    private lazy val ackErrorMeter = metrics.meter("ack-error", "ack-error", member.scopeName)

    import SessionProtocol._

    private val pushScheduler = new Scheduler(this, PushNext, 1000, 1000 / pushTps,
      blockingMessage = true, autoStart = false, name = Some("MasterSessionActor.Push"))
    private var pendingSequences: TreeMap[Long, Option[Timestamp]] = TreeMap()
    private var lastSendTime = currentTime
    private var lastSequence = 0L
    private var isTerminating = false
    private var maxSlaveAckTimestamp: Option[Timestamp] = startTimestamp
    private var activeSlaveTimestamp: Option[Timestamp] = startTimestamp

    def replicationLagSeconds: Option[Int] = for {
      lastTs <- activeSlaveTimestamp
      consistentTs <- getMemberCurrentConsistentTimestamp(member)
    } yield ((consistentTs.timeMs - lastTs.timeMs) / 1000).toInt

    private def updateLag(timestamp: Timestamp): Unit = {

      // Compute max acknowledged slave timestamp.
      val newMaxTs = Ordering[Timestamp].max(maxSlaveAckTimestamp.getOrElse(timestamp), timestamp)
      maxSlaveAckTimestamp = Some(newMaxTs)

      // Compute the new active slave timestamp. This is a safety in case the master receives slave's acknowledgement
      // out of order. The active slave timestamp cannot be more recent than an unacknowledged outstanding transaction.
      val newActiveTs = pendingSequences.collectFirst { case (_, Some(ts)) => ts} match {
        case Some(firstPendingTs) if firstPendingTs > newMaxTs => maxSlaveAckTimestamp
        case Some(firstPendingTs) if firstPendingTs > timestamp => Some(timestamp)
        case Some(_) => activeSlaveTimestamp
        case None => maxSlaveAckTimestamp
      }

      if (activeSlaveTimestamp != newActiveTs) {
        activeSlaveTimestamp = newActiveTs
        replicationLagSeconds.foreach(lag => notifyObservers(ReplicationLagChanged(member.token, slave, lag)))
      }
    }

    private def nextSequence = {
      lastSequence += 1
      lastSequence
    }

    override def start() = {
      super.start()
      if (pushTps > 0) {
        pushScheduler.start()
      }
      this
    }

    private def currentWindowSize: Int = {
      pendingSequences.headOption match {
        case Some((firstPendingSequence, _)) => (lastSequence - firstPendingSequence).toInt + 1
        case None => 0
      }
    }

    private def sendKeepAlive() {
      pushMessage(None)
    }

    private def pushMessage(transaction: Option[Message]) {

      def onReply(sequence: Long)(response: Message, optException: Option[Exception]) {
        optException match {
          case Some(e) => {
            debug("Received an error response from the slave (seq={}): ", sequence, e)
            manager ! SessionManagerProtocol.TerminateSession(SessionActor.this, Some(e))
            isTerminating = true
          }
          case None => {
            trace("Received an push response from the slave (seq={}).", sequence)
            this ! Ack(sequence)
          }
        }
      }

      val sequence = nextSequence
      var params: Map[String, MValue] = Map()
      val data = transaction match {
        case Some(message) => {
          val timestamp = message.timestamp.get // Must fail if timestamp is missing
          params += (ReplicationAPIParams.Timestamp -> timestamp.value)
          pendingSequences += (sequence -> message.timestamp)
          message
        }
        case None => {
          // Keep-alive
          pendingSequences += (sequence -> None)
          null
        }
      }
      params += (Sequence -> sequence)
      params += (SessionId -> sessionId)
      params += (ConsistentTimestamp -> getMemberCurrentConsistentTimestamp(member).get.value)

      val pushMessage = new OutMessage(params = params, data = data,
        onReply = onReply(sequence), responseTimeout = service.responseTimeout)
      pushMessage.destination = new Endpoints(Seq(new Shard(-1, Seq(new Replica(-1, slave)))))
      pushAction.call(pushMessage)

      lastSendTime = currentTime
      pushMeter.mark()

      trace("Replicated message to slave (seq={}, window={}).", sequence, currentWindowSize)
    }

    private def verifyAckIdleness(): Unit = {
      val elapsedTime = currentTime - lastSendTime
      if (elapsedTime > maxIdleDurationInMs) {
        ackTimeoutMeter.mark()
        info("No ack received for {} ms. Terminating session {} for {}.", elapsedTime, sessionId, member)
        manager ! SessionManagerProtocol.TerminateSession(SessionActor.this, None)
        isTerminating = true
      }
    }

    def act() {
      loop {
        react {
          case PushNext if !isTerminating => {
            try {
              if (currentWindowSize < pushWindowSize) {
                if (source.hasNext) {
                  source.next() match {
                    case Some(txMessage) => pushMessage(Some(txMessage))
                    case None => {
                      // No more message available at this time but the replication source is not empty
                      // (i.e. hasNext == true). We are expecting more transaction messages to be available soon.
                      // Meanwhile send to slave a keep-alive message every few seconds to prevent session
                      // idle timeout.
                      if (currentTime - lastSendTime > math.min(maxIdleDurationInMs / 4, 5000)) {
                        sendKeepAlive()
                      }
                    }
                  }
                } else if (pendingSequences.isEmpty) {
                  info("Replication source is exhausted! Terminating session {} for {} (lag={}).",
                    sessionId, member, replicationLagSeconds)
                  manager ! SessionManagerProtocol.TerminateSession(SessionActor.this, None)
                  isTerminating = true
                } else {
                  // Replication source is empty. Close session if haven't received an ack for a while.
                  verifyAckIdleness()
                }
              } else {
                // Window size is full. Close session if haven't received an ack for a while.
                verifyAckIdleness()
              }
            } catch {
              case e: Exception => {
                pushErrorMeter.mark()
                info("Error pushing a transaction to slave (subid={}). {}: ", sessionId, member, e)
                manager ! SessionManagerProtocol.TerminateSession(SessionActor.this, Some(e))
                isTerminating = true
              }
            } finally {
              reply(true)
            }
          }
          case Ack(sequence) if !isTerminating => {
            try {
              ackMeter.mark()
              val timestamp = pendingSequences(sequence)
              pendingSequences -= sequence

              // Update lag with last acknowledged slave timestamp
              timestamp.foreach { ts =>
                updateLag(ts)
                trace("Successfully acknowledged transaction (subid={}, seq={}, ts={}). {}", sessionId, sequence, ts, member)
              }
            } catch {
              case e: Exception => {
                ackErrorMeter.mark()
                info("Error acknowledging transaction (subid={}, seq={}). {}: ", sessionId, sequence, member, e)
                manager ! SessionManagerProtocol.TerminateSession(SessionActor.this, Some(e))
                isTerminating = true
              }
            }
          }
          case Kill => {
            try {
              try {
                pushScheduler.cancel()
                source.close()
              } finally {
                exit()
              }
            } catch {
              case e: Exception => {
                info("Error killing session actor (subid={}). {}: ", sessionId, member, e)
              }
            } finally {
              reply(true)
            }
          }
          case _ if isTerminating => {
            debug("Ignore actor message since session {} is terminating. {}", sessionId, member)
          }
        }
      }
    }
  }

}

object MasterReplicationSessionManager {

  case class ReplicationLagChanged(token: Long, slave: Node, replicationLagSeconds: Int) extends Event
}
