package com.wajam.nrv.consistency.replication

import com.wajam.nrv.service._
import com.wajam.nrv.utils.timestamp.Timestamp
import com.wajam.nrv.data._
import com.wajam.nrv.consistency.{ConsistentStore, ResolvedServiceMember}
import com.wajam.nrv.consistency.log.TransactionLog
import com.wajam.nrv.cluster.Node
import com.wajam.nrv.utils.{CurrentTime, Scheduler, UuidStringGenerator}
import scala.actors.Actor
import collection.immutable.TreeSet
import ReplicationAPIParams._
import com.wajam.nrv.Logging
import com.yammer.metrics.scala.Instrumented

/**
 * Manage all the local master replication sessions the local service is acting as a replication source. The replication
 * source must be the master replica and the replication source must have transaction logs enabled.
 */
class MasterReplicationSessionManager(service: Service, store: ConsistentStore,
                           getTransactionLog: (ResolvedServiceMember) => TransactionLog,
                           getMemberCurrentConsistentTimestamp: (ResolvedServiceMember) => Option[Timestamp],
                           pushAction: Action, pushTps: Int, pushWindowSize: => Int, maxIdleDurationInMs: Long)
  extends CurrentTime with UuidStringGenerator with Logging with Instrumented {

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

              val session = new SessionActor(nextId, member, source, cookie.getOrElse(""), message.source)
              sessions = session :: sessions

              // Reply with an open session response
              val response = new OutMessage()
              response.parameters += (SessionId -> session.sessionId)
              response.parameters += (Start -> start)
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
                ReplicationSession(sessionActor.member, sessionActor.cookie, mode,
                  Some(sessionActor.sessionId), Some(sessionActor.source.start), sessionActor.source.end)
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

  class SessionActor(val sessionId: String, val member: ResolvedServiceMember, val source: ReplicationSourceIterator,
                          val cookie: String, slave: Node) extends Actor with Logging {

    private lazy val pushMeter = metrics.meter("push", "push", member.scopeName)
    private lazy val pushErrorMeter = metrics.meter("push-error", "push-error", member.scopeName)

    private lazy val ackMeter = metrics.meter("ack", "ack", member.scopeName)
    private lazy val ackTimeoutMeter = metrics.meter("ack-timeout", "ack-timeout", member.scopeName)
    private lazy val ackErrorMeter = metrics.meter("ack-error", "ack-error", member.scopeName)

    import SessionProtocol._

    private val pushScheduler = new Scheduler(this, PushNext, 1000, 1000 / pushTps,
      blockingMessage = true, autoStart = false, name = Some("MasterSessionActor.Push"))
    private var pendingSequences: TreeSet[Long] = TreeSet()
    private var lastSendTime = currentTime
    private var lastSequence = 0L
    private var isTerminating = false

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
        case Some(firstPendingSequence) => (lastSequence - firstPendingSequence).toInt + 1
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
          message
        }
        case None => null // Keep-alive
      }
      params += (Sequence -> sequence)
      params += (SessionId -> sessionId)

      val pushMessage = new OutMessage(params = params, data = data,
        onReply = onReply(sequence), responseTimeout = service.responseTimeout)
      pushMessage.destination = new Endpoints(Seq(new Shard(-1, Seq(new Replica(-1, slave)))))
      pushAction.call(pushMessage)

      pendingSequences += sequence
      lastSendTime = currentTime
      pushMeter.mark()

      trace("Replicated message to slave (seq={}, window={}).", sequence, currentWindowSize)
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
                } else {
                  info("Replication source is exhausted! Terminating session {} for {}.", sessionId, member)
                  manager ! SessionManagerProtocol.TerminateSession(SessionActor.this, None)
                  isTerminating = true
                }
              } else {
                // Window size is full. Close session if haven't received an ack for a while.
                val elapsedTime = currentTime - lastSendTime
                if (elapsedTime > maxIdleDurationInMs) {
                  ackTimeoutMeter.mark()
                  info("No ack received for {} ms. Terminating session {} for {}.", elapsedTime, sessionId, member)
                  manager ! SessionManagerProtocol.TerminateSession(SessionActor.this, None)
                  isTerminating = true
                }
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
              pendingSequences -= sequence
              ackMeter.mark()
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
