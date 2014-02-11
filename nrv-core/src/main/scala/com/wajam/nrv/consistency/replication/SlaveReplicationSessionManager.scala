package com.wajam.nrv.consistency.replication

import com.wajam.nrv.service.{Action, Service}
import com.wajam.nrv.consistency.{ConsistencyException, ResolvedServiceMember, ConsistentStore}
import com.wajam.nrv.data.{MValue, InMessage, Message}
import scala.actors.Actor
import com.wajam.nrv.utils._
import collection.immutable.TreeSet
import com.wajam.nrv.consistency.log.TransactionLog
import ReplicationAPIParams._
import com.wajam.nrv.consistency.log.LogRecord.{Response, Request, Index}
import com.wajam.nrv.consistency.log.LogRecord.Response.Success
import annotation.tailrec
import com.wajam.nrv.TimeoutException
import java.util.{TimerTask, Timer}
import com.yammer.metrics.scala.Instrumented
import util.Random
import com.wajam.commons._
import com.wajam.nrv.utils.timestamp.Timestamp

/**
 * Manage all local slave replication sessions for a service. Only one replication session per
 * service member is allowed.
 */
class SlaveReplicationSessionManager(service: Service, store: ConsistentStore, maxIdleDurationInMs: Long, commitFrequency: Int,
                            logIdGenerator: => IdGenerator[Long] = new TimestampIdGenerator)
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

  /**
   * Open session can be delayed i.e. the call to the master service member is done after
   * a specified amount of time. This class is keeping track of the pending sessions (i.e. delayed or awaiting
   * master response). All new open session calls for a given member are silently ignored if a session
   * (pending or active) already exists for the member.
   */
  def openSession(member: ResolvedServiceMember, txLog: TransactionLog, delay: Long, openSessionAction: Action,
                closeSessionAction: Action, mode: ReplicationMode, onSessionEnd: Option[Exception] => Unit) {
    val cookie = nextId
    manager ! SessionManagerProtocol.OpenSessionRequest(member, txLog, openSessionAction, closeSessionAction, mode,
      cookie, onSessionEnd, delay)
  }

  def closeSession(member: ResolvedServiceMember) {
    manager ! SessionManagerProtocol.CloseSessionRequest(member)
  }

  /**
   * Process replication push message received from the master replica.
   */
  def handleReplicationMessage(message: InMessage) {
    manager ! SessionManagerProtocol.TransactionPush(message)
  }

  def sessions: Iterable[ReplicationSession] = {
    val allSessions = manager !? SessionManagerProtocol.GetSessions
    allSessions.asInstanceOf[Iterable[ReplicationSession]]
  }

  object SessionManagerProtocol {

    case class OpenSessionRequest(member: ResolvedServiceMember, txLog: TransactionLog, openSessionAction: Action,
                         closeSessionAction: Action, mode: ReplicationMode, cookie: String,
                         onSessionEnd: Option[Exception] => Unit, delay: Long = 0)

    case class OpenSessionResponse(context: OpenSessionRequest, response: Message, optException: Option[Exception])

    case class CloseSessionRequest(member: ResolvedServiceMember)

    object GetSessions

    case class TransactionPush(message: InMessage)

    case class TerminateSession(sessionActor: SessionActor, error: Option[Exception] = None)

    object Kill

  }

  class SessionManagerActor extends Actor {

    private lazy val openSessionMeter = metrics.meter("open-session", "open-session", serviceScope)
    private lazy val openSessionIgnoreMeter = metrics.meter("open-session-ignore", "open-session-ignore", serviceScope)
    private lazy val openSessionOkMeter = metrics.meter("open-session-ok", "open-session-ok", serviceScope)
    private lazy val openSessionErrorMeter = metrics.meter("open-session-error", "open-session-error", serviceScope)

    private lazy val closeSessionMeter = metrics.meter("close-session", "close-session", serviceScope)
    private lazy val closeSessionErrorMeter = metrics.meter("close-session-error", "close-session-error", serviceScope)

    private lazy val pushMeter = metrics.meter("push", "push", serviceScope)
    private lazy val pushIgnoreMeter = metrics.meter("push-ignore", "push-ignore", serviceScope)
    private lazy val pushErrorMeter = metrics.meter("push-error", "push-error", serviceScope)

    private lazy val terminateErrorMeter = metrics.meter("terminate-error", "terminate-error", serviceScope)

    import SessionManagerProtocol._

    private case class PendingOpenSessionRequest(context: OpenSessionRequest)

    private val timer = new Timer("SlaveSessionManagerActor-Timer")

    private var sessions: Map[ResolvedServiceMember, Either[PendingOpenSessionRequest, SessionActor]] = Map()

    def sessionsCount = sessions.size

    private var sessionGaugeCache: Map[ResolvedServiceMember, SessionCurrentTimestampGauge] = Map()

    private def getSessionGauge(member: ResolvedServiceMember) = sessionGaugeCache.getOrElse(member, {
      val gauge = new SessionCurrentTimestampGauge(member)
      sessionGaugeCache += (member -> gauge)
      gauge
    })

    private def terminateSession(session: SessionActor, error: Option[Exception]) {
      sessions -= session.member
      session ! SessionProtocol.Kill

      sendCloseSession(session.context, session.sessionId)
    }

    private def sendCloseSession(context: OpenSessionRequest, sessionId: String) {
      def handleCloseSessionResponse(reponse: InMessage, error: Option[Exception]) {
        error match {
          case Some(e) => info("Close session reponse error {} for {}: {}", sessionId, context.member, e)
          case None => debug("Close session reponse {} for {}", sessionId, context.member)
        }
      }

      info("Send close session request to master {}. {}", sessionId, context.member)
      var params: Map[String, MValue] = Map()

      params += (ReplicationAPIParams.SessionId -> sessionId)
      params += (ReplicationAPIParams.Token -> context.member.token.toString)
      context.closeSessionAction.call(params, onReply = handleCloseSessionResponse(_, _))
    }

    def act() {
      loop {
        react {
          case context: OpenSessionRequest => {
            try {
              openSessionMeter.mark()
              sessions.get(context.member) match {
                case Some(Left(_)) => {
                  openSessionIgnoreMeter.mark()
                  info("Ignore new open session request. Already have a pending session for {}.",
                    context.member)
                }
                case Some(Right(_)) => {
                  openSessionIgnoreMeter.mark()
                  info("Ignore new open session request. Already have an open session for {}.",
                    context.member)
                }
                case None => {
                  info("Registering a new pending open session for {}", context.member)
                  timer.schedule(new TimerTask {
                    def run() {
                      SessionManagerActor.this ! PendingOpenSessionRequest(context)
                    }
                  }, context.delay)
                  sessions += (context.member -> Left(PendingOpenSessionRequest(context)))
                }
              }
            } catch {
              case e: Exception => {
                openSessionErrorMeter.mark()
                warn("Error processing open session for {}", context.member, e)
                sessions -= context.member
                context.onSessionEnd(Some(e))
              }
            }
          }
          case PendingOpenSessionRequest(context) => {
            try {
              sessions.get(context.member) match {
                case Some(Left(_)) => {
                  info("Send open session request to master for pending session {}", context.member)
                  var params: Map[String, MValue] = Map()
                  params += (ReplicationAPIParams.Token -> context.member.token.toString)
                  params += (ReplicationAPIParams.Cookie -> context.cookie)
                  context.txLog.getLastLoggedRecord.map(_.consistentTimestamp) match {
                    case Some(Some(lastTimestamp)) => {
                      params += (ReplicationAPIParams.Start -> lastTimestamp.toString)
                    }
                    case _ => {
                      // No records in transaction log. Omit start if the local store is empty.
                      // The replication publisher will send all the transactions from the beginning.
                    }
                  }
                  params += (ReplicationAPIParams.Mode -> context.mode.toString)
                  debug("openSessionAction.call {}", params)
                  context.openSessionAction.call(params,
                    onReply = SessionManagerActor.this ! OpenSessionResponse(context, _, _))
                }
                case Some(Right(_)) => {
                  openSessionIgnoreMeter.mark()
                  warn("Do not process pending open session request. Already have an active session for {}.",
                    context.member)
                }
                case None => {
                  openSessionIgnoreMeter.mark()
                  info("Do not process pending open session request. No more pending session registered for {}.",
                    context.member)
                }
              }
            } catch {
              case e: Exception => {
                openSessionErrorMeter.mark()
                warn("Error processing delayed open session request for {}", context.member, e)
                sessions -= context.member
                context.onSessionEnd(Some(e))
              }
            }
          }
          case OpenSessionResponse(context, message, exception) => {
            try {
              implicit val response = message

              exception match {
                case Some(e) => {
                  openSessionErrorMeter.mark()
                  warn("Got an open session response error for {}: ", context.member, e)
                  sessions -= context.member
                  context.onSessionEnd(Some(e))
                }
                case None => {
                  val sessionId = getSessionId(message)
                  val startTimestamp = Timestamp(getParamLongValue(Start))
                  val endTimestamp = getOptionalParamLongValue(End).map(ts => Timestamp(ts))
                  val masterConsistentTimestamp = getOptionalParamLongValue(ConsistentTimestamp).map(ts => Timestamp(ts))
                  val cookie = getParamStringValue(Cookie)

                  sessions.get(context.member) match {
                    case Some(Left(_)) if Some(startTimestamp) == endTimestamp => {
                      openSessionIgnoreMeter.mark()
                      info("Do not activate session. " +
                        "Session is empty i.e. has identical start/end timestamps) {}: {}",
                        response, context.member)

                      sessions -= context.member
                      sendCloseSession(context, sessionId)
                      context.onSessionEnd(None)
                    }
                    case Some(Left(pending)) if cookie == pending.context.cookie => {
                      info("Open session response {}. Activate session {}.", response, context.member)

                      val session = new SessionActor(sessionId, startTimestamp, endTimestamp, context,
                        getSessionGauge(context.member), logIdGenerator, masterConsistentTimestamp)
                      sessions += (context.member -> Right(session))
                      session.start()
                      openSessionOkMeter.mark()
                    }
                    case Some(Left(_)) => {
                      warn("Do not activate session. Received open session response with wrong cookie. {} {}",
                        context.member, response)
                      openSessionIgnoreMeter.mark()

                      sendCloseSession(context, sessionId)
                    }
                    case Some(Right(_)) => {
                      openSessionIgnoreMeter.mark()
                      warn("Do not activate session. Already have an active session for {}.",
                        context.member)

                      sendCloseSession(context, sessionId)
                    }
                    case None => {
                      openSessionIgnoreMeter.mark()
                      info("Do not activate session. No matching pending open session request for {} {}.",
                        context.member, context.cookie)

                      sendCloseSession(context, sessionId)
                    }
                  }
                }
              }
            } catch {
              case e: Exception => {
                openSessionErrorMeter.mark()
                warn("Error processing open session response for {}", context.member, e)
                sessions -= context.member
                context.onSessionEnd(Some(e))
              }
            }
          }
          case CloseSessionRequest(member) => {
            try {
              sessions.get(member) match {
                case Some(Left(PendingOpenSessionRequest(_))) => {
                  closeSessionMeter.mark()
                  info("Close session request. Remove pending session. {}", member)
                  sessions -= member
                }
                case Some(Right(sessionActor)) => {
                  closeSessionMeter.mark()
                  info("Close session request. Remove active session for. {}", member)
                  terminateSession(sessionActor, None)
                }
                case None => {
                  debug("Close session request. No action taken, no session open for {}.", member)
                }
              }
            } catch {
              case e: Exception => {
                closeSessionErrorMeter.mark()
                warn("Error processing close session request for {}", member, e)
              }
            }
          }
          case GetSessions => {
            try {
              val allSessions = sessions.valuesIterator.map {
                case Left(pendingSession) => {
                  val context = pendingSession.context
                  ReplicationSession(context.member, context.cookie, context.mode, service.cluster.localNode)
                }
                case Right(sessionActor) => {
                  val context = sessionActor.context
                  ReplicationSession(sessionActor.member, context.cookie, context.mode, service.cluster.localNode,
                    Some(sessionActor.sessionId), Some(sessionActor.startTimestamp), sessionActor.endTimestamp, sessionActor.replicationLagInS)
                }
              }
              reply(allSessions.toList)
            } catch {
              case e: Exception => {
                warn("Error processing get sessions {}", e)
                reply(Nil)
              }
            }
          }
          case TransactionPush(message) => {
            try {
              val sessionId = getSessionId(message)
              sessions.collectFirst({
                case (member, Right(sessionActor)) if sessionActor.sessionId == sessionId => sessionActor
              }) match {
                case Some(sessionActor) => {
                  pushMeter.mark()
                  if (message.hasData) {
                    sessionActor ! SessionProtocol.TransactionMessage(message)
                  } else {
                    sessionActor ! SessionProtocol.KeepAliveMessage(message)
                  }
                }
                case None => {
                  pushIgnoreMeter.mark()
                  // Cannot send close session to master, we do not have access to the close session Action
                }
              }
            } catch {
              case e: Exception => {
                pushErrorMeter.mark()
                warn("Error processing pushed transaction {}", message, e)
              }
            }
          }
          case TerminateSession(sessionActor, error) => {
            try {
              sessions.get(sessionActor.member) match {
                case Some(Right(session)) if session.sessionId == sessionActor.sessionId => {
                  info("Session actor {} wants to be terminated. Dutifully perform euthanasia! {}",
                    sessionActor.sessionId, sessionActor.member)
                  terminateSession(sessionActor, error)
                  session.context.onSessionEnd(error)
                }
                case _ => // No matching open session anymore. No action taken.
              }
            } catch {
              case e: Exception => {
                terminateErrorMeter.mark()
                warn("Error processing terminate session. {}", sessionActor.member, e)
              }
            }
          }
          case Kill => {
            try {
              sessions.valuesIterator.foreach {
                case Right(sessionActor) => sessionActor !? SessionProtocol.Kill
                case _ =>
              }
              sessions = Map()
              timer.cancel()
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

    trait ReplicationMessage extends Ordered[ReplicationMessage] {
      def message: InMessage

      def sequence: Long

      def compare(that: ReplicationMessage) = sequence.compare(that.sequence)

      def reply() {
        message.reply(Nil)
      }
    }

    case class TransactionMessage(message: InMessage) extends ReplicationMessage {
      val sequence: Long = getParamLongValue(Sequence)(message)
      val timestamp: Timestamp = getParamLongValue(ReplicationAPIParams.Timestamp)(message)
      val masterConsistentTimestamp: Option[Timestamp] = getOptionalParamLongValue(ConsistentTimestamp)(message).map(ts => Timestamp(ts))
      val transaction: Message = message.getData[Message]
      val token = transaction.token

      transaction.timestamp = Some(timestamp)
    }

    case class KeepAliveMessage(message: InMessage) extends ReplicationMessage {
      val sequence: Long = getParamLongValue(Sequence)(message)
    }

    object CheckIdle

    object Commit

    object Kill

  }

  /**
   * Current timestamp gauge wrapper class per service member. Gauge with the same name and scope are not registered
   * more than once. If the gauge was directly referenced by the SessionActor, the value would not be updated
   * by the future sessions of the same member. This would also pin the SessionActor in the heap forever since
   * the gauge keep a reference to the code block providing the value. <p></p> The manager keeps one gauge wrapper per
   * member forever.
   */
  class SessionCurrentTimestampGauge(member: ResolvedServiceMember) {
    var timestamp: Timestamp = Timestamp(0)

    private val gauge = metrics.gauge("current-timestamp", member.scopeName) {
      timestamp.value
    }
  }

  class SessionActor(val sessionId: String,
                     val startTimestamp: Timestamp,
                     val endTimestamp: Option[Timestamp],
                     val context: SessionManagerProtocol.OpenSessionRequest,
                     currentTimestampGauge: SessionCurrentTimestampGauge,
                     idGenerator: IdGenerator[Long],
                     initialMasterConsistentTimestamp: Option[Timestamp])
    extends Actor {

    private lazy val txReceivedMeter = metrics.meter("tx-received", "tx-received", member.scopeName)
    private lazy val txMissingMeter = metrics.meter("tx-missing", "tx-missing", member.scopeName)
    private lazy val idletimeoutMeter = metrics.meter("idle-timeout", "idle-timeout", member.scopeName)
    private lazy val errorMeter = metrics.meter("error", "error", member.scopeName)
    private lazy val keepAliveMeter = metrics.meter("keep-alive", "keep-alive", member.scopeName)
    private lazy val txWriteTimer = metrics.timer("tx-write", member.scopeName)

    import SessionProtocol._

    val commitScheduler = if (commitFrequency > 0) {
      Some(new Scheduler(this, Commit, Random.nextInt(commitFrequency), commitFrequency, blockingMessage = true,
        autoStart = false, name = Some("SlaveSessionActor.Commit")))
    } else {
      None
    }
    private val checkIdleScheduler = new Scheduler(this, CheckIdle, 1000, 1000, blockingMessage = true,
      autoStart = false, name = Some("SlaveSessionActor.CheckIdle"))
    private var pendingTransactions: TreeSet[ReplicationMessage] = TreeSet()
    private var consistentTimestamp: Option[Timestamp] = txLog.getLastLoggedRecord match {
      case Some(record) => record.consistentTimestamp
      case None => None
    }
    private var lastSequence = 0L
    private var lastReceiveTime = currentTime
    private var terminating = false

    private def txLog = context.txLog

    def member = context.member

    private var masterConsistentTimestamp: Option[Timestamp] = initialMasterConsistentTimestamp

    def replicationLagInS: Option[Int] = for {
      slaveConsistentTs <- consistentTimestamp
      masterConsistentTs <- masterConsistentTimestamp
    } yield ((masterConsistentTs.value - slaveConsistentTs.value) / 1000).toInt

    override def start() = {
      super.start()
      commitScheduler.foreach(_.start())
      checkIdleScheduler.start()
      this
    }

    /**
     * Add the head pending transaction to the transaction log and the consistent store if its sequence number follows
     * directly the last added transaction sequence number. No gap in sequence is allowed to prevent skipping
     * transactions.
     */
    @tailrec
    private def processHeadMessage() {
      pendingTransactions.headOption match {
        case Some(replicationMessage) if replicationMessage.sequence == lastSequence + 1 => {
          pendingTransactions = pendingTransactions.tail

          replicationMessage match {
            case tx: TransactionMessage => {
              // Add transaction message to transaction log and to consistent storage.
              trace("Storing (seq={}, sessionId={}) {}", tx.sequence, sessionId, tx.transaction)
              txWriteTimer.time {
                txLog.append {
                  Request(idGenerator.nextId, consistentTimestamp, tx.timestamp, tx.token, tx.transaction)
                }
                store.writeTransaction(tx.transaction)
                txLog.append {
                  Response(idGenerator.nextId, consistentTimestamp, tx.timestamp, tx.token, Success)
                }
              }

              // Update consistent timestamp
              consistentTimestamp = Some(tx.timestamp)
              currentTimestampGauge.timestamp = tx.timestamp

              // Save the master's consistent timestamp
              masterConsistentTimestamp = tx.masterConsistentTimestamp
            }
            case _: KeepAliveMessage => {
              keepAliveMeter.mark()
              trace("Keep alive (seq={}, sessionId={})", replicationMessage.sequence, sessionId)
            }
          }

          lastSequence = replicationMessage.sequence
          replicationMessage.reply()

          if (endTimestamp.nonEmpty && consistentTimestamp == endTimestamp) {
            info("Processed last session transaction. Terminating session {} for {}.", sessionId, member)
            txLog.append {
              Index(idGenerator.nextId, consistentTimestamp)
            }
            manager ! SessionManagerProtocol.TerminateSession(SessionActor.this, None)
            terminating = true
          } else {
            // Process the new head recursively
            processHeadMessage()
          }
        }
        case _ => // Do nothing as the head transaction is empty or not the next sequence number.
      }
    }

    def act() {
      loop {
        react {
          case message: ReplicationMessage if !terminating => {
            try {
              trace("Received replication message (seq={}, sessionId={}) {}", message.sequence, sessionId, message)
              txReceivedMeter.mark()
              pendingTransactions += message
              lastReceiveTime = currentTime

              processHeadMessage()
            } catch {
              case e: Exception => {
                warn("Error processing replication message {}: ", message, e)
                errorMeter.mark()
                manager ! SessionManagerProtocol.TerminateSession(SessionActor.this, Some(e))
                terminating = true
              }
            }
          }
          case CheckIdle if !terminating => {
            try {
              val elapsedTime = currentTime - lastReceiveTime
              if (elapsedTime > maxIdleDurationInMs) {
                val error = if (pendingTransactions.isEmpty) {
                  idletimeoutMeter.mark()
                  info("No pending transaction received for {} ms. Terminating session {} for {}.",
                    elapsedTime, sessionId, member)
                  new TimeoutException("No pending transaction received", Some(elapsedTime))
                } else {
                  txMissingMeter.mark()
                  info("Missing transactions (last written sequence {}, available sequences {}) after {} ms. " +
                    "Terminating session {} for {}.",
                    lastSequence, pendingTransactions.map(_.sequence), elapsedTime, sessionId, member)
                  new ConsistencyException()
                }

                manager ! SessionManagerProtocol.TerminateSession(SessionActor.this, Some(error))
                terminating = true
              }
            } catch {
              case e: Exception => {
                warn("Error checking for idle session {} for {}: ", sessionId, member, e)
                errorMeter.mark()
                manager ! SessionManagerProtocol.TerminateSession(SessionActor.this, Some(e))
                terminating = true
              }
            } finally {
              reply(true)
            }
          }
          case Commit => {
            try {
              trace("Commit transaction log: {}", txLog)
              txLog.commit()
            } catch {
              case e: Exception => {
                errorMeter.mark()
                warn("Error committing session {} transaction log for {}: ", sessionId, member, e)
                manager ! SessionManagerProtocol.TerminateSession(SessionActor.this, Some(e))
                terminating = true
              }
            } finally {
              reply(true)
            }
          }
          case Kill => {
            try {
              debug("Killing session {} actor {}: ", sessionId, member)
              checkIdleScheduler.cancel()
              commitScheduler.foreach(_.cancel())
              exit()
            } catch {
              case e: Exception => {
                warn("Error killing session {} actor {}: ", sessionId, member, e)
              }
            } finally {
              reply(true)
            }
          }
          case _ if terminating => {
            debug("Ignore actor message since session {} is terminating. {}", sessionId, member)
          }
        }
      }
    }
  }

}