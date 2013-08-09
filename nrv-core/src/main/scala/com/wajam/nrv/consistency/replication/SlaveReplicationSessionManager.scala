package com.wajam.nrv.consistency.replication

import com.wajam.nrv.service.{TokenRange, Action, Service}
import com.wajam.nrv.consistency.{ConsistencyException, ResolvedServiceMember, ConsistentStore}
import com.wajam.nrv.data.{MValue, InMessage, Message}
import com.wajam.nrv.utils.timestamp.Timestamp
import scala.actors.Actor
import com.wajam.nrv.utils._
import collection.immutable.TreeSet
import com.wajam.nrv.consistency.persistence.TransactionLog
import ReplicationAPIParams._
import com.wajam.nrv.consistency.persistence.LogRecord.{Response, Request}
import com.wajam.nrv.consistency.persistence.LogRecord.Response.Success
import annotation.tailrec
import com.wajam.nrv.{TimeoutException, Logging}
import java.util.{TimerTask, Timer}
import com.yammer.metrics.scala.Instrumented
import util.Random
import com.wajam.nrv.consistency.persistence.LogRecord.Index

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
   * Subscribe can be delayed i.e. the subscribe call to the master service member is done after
   * a specified amount of time. This class is keeping track of the pending sessions (i.e. delayed or awaiting
   * master response). All new subscribe calls for a given member are silently ignored if a session
   * (pending or active) already exists for the member.
   */
  def subscribe(member: ResolvedServiceMember, txLog: TransactionLog, delay: Long, subscribeAction: Action,
                unsubscribeAction: Action, mode: ReplicationMode, onSessionEnd: Option[Exception] => Unit) {
    val cookie = nextId
    manager ! SessionManagerProtocol.Subscribe(member, txLog, subscribeAction, unsubscribeAction, mode,
      cookie, onSessionEnd, delay)
  }

  def unsubscribe(member: ResolvedServiceMember) {
    manager ! SessionManagerProtocol.Unsubscribe(member)
  }

  /**
   * Process replication push message received from the master replica.
   */
  def handleReplicationMessage(message: InMessage) {
    manager ! SessionManagerProtocol.TransactionPush(message)
  }

  def sessions: Iterable[ReplicationSession] = {
    val subs = manager !? SessionManagerProtocol.GetSessions
    subs.asInstanceOf[Iterable[ReplicationSession]]
  }

  private def firstStoreRecord(from: Option[Timestamp], ranges: Seq[TokenRange]): Option[Message] = {
    val itr = from match {
      case Some(timestamp) => store.readTransactions(timestamp, Long.MaxValue, ranges)
      case None => store.readTransactions(Long.MinValue, Long.MaxValue, ranges)
    }
    try {
      itr.find(_ => true)
    } finally {
      itr.close()
    }
  }

  object SessionManagerProtocol {

    case class Subscribe(member: ResolvedServiceMember, txLog: TransactionLog, subscribeAction: Action,
                         unsubscribeAction: Action, mode: ReplicationMode, cookie: String,
                         onSessionEnd: Option[Exception] => Unit, delay: Long = 0)

    // TODO: Either[Exception, Message]???
    case class SubscribeResponse(subscribe: Subscribe, response: Message, optException: Option[Exception])

    case class Unsubscribe(member: ResolvedServiceMember)

    object GetSessions

    case class TransactionPush(message: InMessage)

    case class TerminateSession(session: SessionActor, error: Option[Exception] = None)

    object Kill

  }

  class SessionManagerActor extends Actor {

    private lazy val subscribeMeter = metrics.meter("subscribe", "subscribe", serviceScope)
    private lazy val subscribeIgnoreMeter = metrics.meter("subscribe-ignore", "subscribe-ignore", serviceScope)
    private lazy val subscribeOkMeter = metrics.meter("subscribe-ok", "subscribe-ok", serviceScope)
    private lazy val subscribeErrorMeter = metrics.meter("subscribe-error", "subscribe-error", serviceScope)

    private lazy val unsubscribeMeter = metrics.meter("unsubscribe", "subscribe", serviceScope)
    private lazy val unsubscribeErrorMeter = metrics.meter("unsubscribe-error", "subscribe-error", serviceScope)

    private lazy val pushMeter = metrics.meter("push", "push", serviceScope)
    private lazy val pushIgnoreMeter = metrics.meter("push-ignore", "push-ignore", serviceScope)
    private lazy val pushErrorMeter = metrics.meter("push-error", "push-error", serviceScope)

    private lazy val terminateErrorMeter = metrics.meter("terminate-error", "terminate-error", serviceScope)

    import SessionManagerProtocol._

    private case class PendingSession(context: Subscribe)

    private val timer = new Timer("SlaveSessionManagerActor-Timer")

    private var sessions: Map[ResolvedServiceMember, Either[PendingSession, SessionActor]] = Map()

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

      sendUnsubscribe(session.context, session.sessionId)
    }

    private def sendUnsubscribe(context: Subscribe, sessionId: String) {
      def handleUnsubscribeResponse(reponse: InMessage, error: Option[Exception]) {
        error match {
          case Some(e) => info("Unsubscribe reponse error {} for {}: {}", sessionId, context.member, e)
          case None => debug("Unsubscribe reponse {} for {}", sessionId, context.member)
        }
      }

      info("Send unsubscribe request to master for session {}. {}", sessionId, context.member)
      var params: Map[String, MValue] = Map()

      params += (ReplicationAPIParams.SubscriptionId -> sessionId)
      params += (ReplicationAPIParams.SessionId -> sessionId)
      params += (ReplicationAPIParams.Token -> context.member.token.toString)
      context.unsubscribeAction.call(params, onReply = handleUnsubscribeResponse(_, _))
    }

    def act() {
      loop {
        react {
          case context: Subscribe => {
            try {
              subscribeMeter.mark()
              sessions.get(context.member) match {
                case Some(Left(_)) => {
                  subscribeIgnoreMeter.mark()
                  info("Ignore new subscribe request. Already have a pending session for {}.",
                    context.member)
                }
                case Some(Right(_)) => {
                  subscribeIgnoreMeter.mark()
                  info("Ignore new subscribe request. Already have an open session for {}.",
                    context.member)
                }
                case None => {
                  info("Registering a new pending session for {}", context.member)
                  timer.schedule(new TimerTask {
                    def run() {
                      SessionManagerActor.this ! PendingSession(context)
                    }
                  }, context.delay)
                  sessions += (context.member -> Left(PendingSession(context)))
                }
              }
            } catch {
              case e: Exception => {
                subscribeErrorMeter.mark()
                warn("Error processing subscribe for {}", context.member, e)
                sessions -= context.member
                context.onSessionEnd(Some(e))
              }
            }
          }
          case PendingSession(context) => {
            try {
              sessions.get(context.member) match {
                case Some(Left(_)) => {
                  info("Send subscribe request to master for pending session. {}", context.member)
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
                      // TODO: prevent replication if store not empty and has no transaction log
                    }
                  }
                  val mode = if (context.mode == ReplicationMode.Bootstrap) ReplicationMode.Store else context.mode
                  params += (ReplicationAPIParams.Mode -> mode.toString)
                  debug("subscribeAction.call {}", params)
                  context.subscribeAction.call(params,
                    onReply = SessionManagerActor.this ! SubscribeResponse(context, _, _))
                }
                case Some(Right(_)) => {
                  subscribeIgnoreMeter.mark()
                  warn("Do not process pending session. Already have an active session for {}.",
                    context.member)
                }
                case None => {
                  subscribeIgnoreMeter.mark()
                  info("Do not process pending session. No more pending session registered for {}.",
                    context.member)
                }
              }
            } catch {
              case e: Exception => {
                subscribeErrorMeter.mark()
                warn("Error processing delayed subscribe for {}", context.member, e)
                sessions -= context.member
                context.onSessionEnd(Some(e))
              }
            }
          }
          case SubscribeResponse(context, message, exception) => {
            try {
              implicit val response = message

              exception match {
                case Some(e) => {
                  subscribeErrorMeter.mark()
                  warn("Got a subscribe response error for {}: ", context.member, e)
                  sessions -= context.member
                  context.onSessionEnd(Some(e))
                }
                case None => {
                  val sessionId = getSessionId(response)
                  val startTimestamp = Timestamp(getParamLongValue(Start))
                  val endTimestamp = getOptionalParamLongValue(End).map(ts => Timestamp(ts))
                  val cookie = getParamStringValue(Cookie)

                  sessions.get(context.member) match {
                    case Some(Left(_)) if Some(startTimestamp) == endTimestamp => {
                      subscribeIgnoreMeter.mark()
                      info("Do not activate session. " +
                        "Session is empty i.e. has identical start/end timestamps) {}: {}",
                        response, context.member)

                      sessions -= context.member
                      sendUnsubscribe(context, sessionId)
                      context.onSessionEnd(None)
                    }
                    case Some(Left(pending)) if cookie == pending.context.cookie => {
                      info("Subscribe response {}. Activate session {}.", response, context.member)

                      val session = new SessionActor(sessionId, startTimestamp, endTimestamp, context,
                        getSessionGauge(context.member), logIdGenerator)
                      sessions += (context.member -> Right(session))
                      session.start()
                      subscribeOkMeter.mark()
                    }
                    case Some(Left(_)) => {
                      warn("Do not activate session. Subscribe response with wrong cookie. {} {}",
                        context.member, response)
                      subscribeIgnoreMeter.mark()

                      sendUnsubscribe(context, sessionId)
                    }
                    case Some(Right(_)) => {
                      subscribeIgnoreMeter.mark()
                      warn("Do not activate session. Already have an active session for {}.",
                        context.member)

                      sendUnsubscribe(context, sessionId)
                    }
                    case None => {
                      subscribeIgnoreMeter.mark()
                      info("Do not activate session. No matching pending session for {} {}.",
                        context.member, context.cookie)

                      sendUnsubscribe(context, sessionId)
                    }
                  }
                }
              }
            } catch {
              case e: Exception => {
                subscribeErrorMeter.mark()
                warn("Error processing subscribe response for {}", context.member, e)
                sessions -= context.member
                context.onSessionEnd(Some(e))
              }
            }
          }
          case Unsubscribe(member) => {
            try {
              sessions.get(member) match {
                case Some(Left(PendingSession(subscribe))) => {
                  unsubscribeMeter.mark()
                  info("Unsubscribe. Remove pending session. {}", member)
                  sessions -= member
                }
                case Some(Right(subscription)) => {
                  unsubscribeMeter.mark()
                  info("Unsubscribe. Remove active session for. {}", member)
                  terminateSession(subscription, None)
                }
                case None => {
                  debug("Unsubscribe. No action taken, no session registered for {}.", member)
                }
              }
            } catch {
              case e: Exception => {
                unsubscribeErrorMeter.mark()
                warn("Error processing unsubscribe for {}", member, e)
              }
            }
          }
          case GetSessions => {
            try {
              val replicationSessions = sessions.valuesIterator.map {
                case Left(pendingSession) => {
                  val context = pendingSession.context
                  ReplicationSession(context.member, context.cookie, context.mode)
                }
                case Right(subscriptionActor) => {
                  val context = subscriptionActor.context
                  ReplicationSession(subscriptionActor.member, context.cookie, context.mode,
                    id = Some(subscriptionActor.sessionId))
                }
              }
              reply(replicationSessions.toList)
            } catch {
              case e: Exception => {
                warn("Error processing get subscriptions {}", e)
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
                  // Cannot unsubscribe from master, we do not have access to unsubscribe Action
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
                case _ => // Not subscribed anymore to that session. No action taken.
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

  class SessionActor(val sessionId: String, startTimestamp: Timestamp, endTimestamp: Option[Timestamp],
                          val context: SessionManagerProtocol.Subscribe,
                          currentTimestampGauge: SessionCurrentTimestampGauge,
                          idGenerator: IdGenerator[Long]) extends Actor {

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
              trace("Storing (seq={}, subId={}) {}", tx.sequence, sessionId, tx.transaction)
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
            }
            case _: KeepAliveMessage => {
              keepAliveMeter.mark()
              trace("Keep alive (seq={}, subId={})", replicationMessage.sequence, sessionId)
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
              trace("Received replication message (seq={}, subId={}) {}", message.sequence, sessionId, message)
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