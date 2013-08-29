package com.wajam.nrv.consistency

import com.wajam.nrv.service._
import com.wajam.nrv.data._
import com.wajam.nrv.utils.timestamp.{Timestamp, TimestampGenerator}
import com.yammer.metrics.scala.{Meter, Instrumented}
import com.wajam.nrv.utils.Event
import com.wajam.nrv.consistency.persistence.{LogRecordSerializer, NullTransactionLog, FileTransactionLog}
import java.util.concurrent.TimeUnit
import com.yammer.metrics.core.Gauge
import com.wajam.nrv.UnavailableException
import com.wajam.nrv.Logging
import com.wajam.nrv.consistency.replication._
import scala.actors.Actor
import com.wajam.nrv.consistency.replication.ReplicationAPIParams._
import com.wajam.nrv.service.StatusTransitionAttemptEvent
import scala.Some
import com.wajam.nrv.service.StatusTransitionEvent

/**
 * Consistency manager for consistent master/slave replication of the binded storage service. The mutation messages are
 * recorded in a transaction log per service member and replicated to slave replicas.
 *
 * TODO: more about transfering the data from log or from store. More about replicas selection.
 *
 * The consistency manager ensure that the storage and the transaction log are always consistent. The consistencyMasterSlave
 * will observe the StatusTransitionAttemptsEvents from the ClusterManager, and will apply a veto on the event if it detects
 * inconsistency, thereby blocking the ServiceMember status transition. It will only allow a service member to go Up if the
 * store and the transaction log are consistent. In case of inconsistency during the service member storage lifetime, the
 * service member status is set to Down. The consistency manager tries to perform the necessary recovery while the service
 * member tries to go up.
 *
 * ASSUMPTIONS:
 * - The messages for a given token are sequenced before reaching the consistency manager.
 * - Messages timestamps are unique in the whole cluster and also sequenced per message token.
 *
 */
class ConsistencyMasterSlave(val timestampGenerator: TimestampGenerator, txLogDir: String, txLogEnabled: Boolean,
                             txLogRolloverSize: Int = 50000000, txLogCommitFrequency: Int = 5000,
                             timestampTimeoutExtraDelay: Int = 250,
                             replicationTps: Int = 50, replicationWindowSize: Int = 20,
                             replicationSessionIdleTimeout: Long = 30000L, replicationOpenSessionDelay: Long = 5000,
                             replicationResolver: Option[Resolver] = None)
  extends Consistency with Logging {

  import SlaveReplicationManagerProtocol._

  private val lastWriteTimestamp = new AtomicTimestamp(AtomicTimestamp.updateIfGreater, None)

  @volatile // updates are synchronized but lookups are not
  private var recorders: Map[Long, TransactionRecorder] = Map()

  private var consistencyStates: Map[Long, MemberConsistencyState] = Map()

  private var metrics: Metrics = null

  private var started = false

  def service: Service with ConsistentStore = bindedServices.head.asInstanceOf[Service with ConsistentStore]

  def resolver = replicationResolver.getOrElse(service.resolver)

  def serializer = new LogRecordSerializer(service.nrvCodec)

  // Mapping between token ranges and service member to speedup consistent timestamp lookup function passed to
  // the consistent storage.
  private var rangeMembers: Map[TokenRange, ServiceMember] = Map()

  private def updateRangeMemberCache() {
    rangeMembers = service.members.flatMap(member => service.getMemberTokenRanges(member).map((_, member))).toMap
  }

  // Slave replication session management
  private lazy val slaveReplicationSessionManager = new SlaveReplicationSessionManager(service, service,
    replicationSessionIdleTimeout, txLogCommitFrequency)
  private lazy val slaveReplicateTxAction = new Action("/replication/slave/sessions/:" + ReplicationAPIParams.SessionId,
    slaveReplicationSessionManager.handleReplicationMessage, ActionMethod.PUT)

  // Master replication session management
  private lazy val masterReplicationSessionManager: MasterReplicationSessionManager = {
    def getTransactionLog(member: ResolvedServiceMember) = recorders.get(member.token) match {
      case Some(recorder) => recorder.txLog
      case None => NullTransactionLog
    }

    def getMemberCurrentConsistentTimestamp(member: ResolvedServiceMember) = recorders.get(member.token) match {
      case Some(recorder) => recorder.currentConsistentTimestamp
      case None => None
    }

    new MasterReplicationSessionManager(service, service, getTransactionLog, getMemberCurrentConsistentTimestamp,
      pushAction = slaveReplicateTxAction, pushTps = replicationTps,
      pushWindowSize = replicationWindowSize, maxIdleDurationInMs = replicationSessionIdleTimeout)
  }

  private lazy val masterOpenSessionAction = new Action("/replication/master/:" + ReplicationAPIParams.Token + "/sessions",
    masterReplicationSessionManager.handleOpenSessionMessage, ActionMethod.POST)
  private lazy val masterCloseSessionAction = new Action("/replication/master/:" + ReplicationAPIParams.Token + "/sessions/:" + ReplicationAPIParams.SessionId,
    masterReplicationSessionManager.handleOpenSessionMessage, ActionMethod.DELETE)

  private lazy val getConsistencyStateAction = new Action("/consistency/state/:" + ReplicationAPIParams.Token,
    handleGetConsistencyState, ActionMethod.GET)

  // replies to a consistency state request, used for external tool assisted consistency checking.
  def handleGetConsistencyState(msg: InMessage) {
    val token = getParamLongValue(Token)(msg)
    val state = consistencyStates.get(token).getOrElse("").toString
    msg.reply(params = Map(Token -> token, "state" -> state), data = null)
  }

  /**
   * Returns local service members with their associated consistency state. Service member without states
   * (e.g. member status Down) are excluded.
   */
  override def localMembersStates: Iterable[(ResolvedServiceMember, MemberConsistencyState)] = {
    consistencyStates.map {
      case (token, state) => (ResolvedServiceMember(service, token), state)
    }
  }

  override def start() {
    synchronized {
      // TODO: update cache when new members are removed (i.e. live shard merge)
      updateRangeMemberCache()

      slaveReplicateTxAction.applySupport(resolver = Some(new Resolver(tokenExtractor = Resolver.TOKEN_RANDOM())),
        nrvCodec = Some(serializer.messageCodec))
      masterOpenSessionAction.applySupport(resolver = replicationResolver, nrvCodec = Some(serializer.messageCodec))
      masterCloseSessionAction.applySupport(resolver = replicationResolver, nrvCodec = Some(serializer.messageCodec))

      masterReplicationSessionManager.start()
      slaveReplicationSessionManager.start()
      SlaveReplicationManagerActor.start()

      // Open a replication session for all service members the local node is a slave
      info("Startup replication open sessions  {}", service)
      service.members.withFilter(member => member.status == MemberStatus.Up && isSlaveReplicaOf(member)).foreach {
        member =>
          SlaveReplicationManagerActor ! OpenSession(member, ReplicationMode.Bootstrap)
      }

      started = true
    }
  }

  override def stop() {
    synchronized {
      if (started) {
        SlaveReplicationManagerActor !? Kill
        slaveReplicationSessionManager.stop()
        masterReplicationSessionManager.stop()
        started = false
      }
    }
  }

  override def bindService(service: Service) {
    require(service.isInstanceOf[ConsistentStore],
      "Consistent service must be type of %s but is %s".format(classOf[ConsistentStore], service.getClass))
    require(bindedServices.size == 0, "Cannot bind to multiple services. Already bound to %s".format(bindedServices.head))

    super.bindService(service)

    metrics = new Metrics(service.name.replace(".", "-"))

    // Setup the current consistent timestamp lookup function to the consistent storage
    info("Setup consistent timestamp lookup for service", service.name)
    this.service.setCurrentConsistentTimestamp(getTokenRangeConsistentTimestamp)

    // Register replication actions
    service.registerAction(slaveReplicateTxAction)
    service.registerAction(masterOpenSessionAction)
    service.registerAction(masterCloseSessionAction)
    service.registerAction(getConsistencyStateAction)
  }

  override def serviceEvent(event: Event) {
    super.serviceEvent(event)

    event match {
      case event: StatusTransitionAttemptEvent if txLogEnabled => {
        handleStatusTransitionAttemptEvent(event)
      }
      case event: StatusTransitionEvent if cluster.isLocalNode(event.member.node) => {
        handleLocalServiceMemberStatusTransitionEvent(event)
      }
      case event: StatusTransitionEvent => {
        handleRemoteServiceMemberStatusTransitionEvent(event)
      }
      case event: NewMemberAddedEvent => {
        updateRangeMemberCache()
        if (event.member.status == MemberStatus.Up && isSlaveReplicaOf(event.member)) {
          SlaveReplicationManagerActor ! OpenSession(event.member, ReplicationMode.Bootstrap)
        }
      }
      case _ => // Ignore unsupported events
    }
  }

  private def getTokenRangeConsistentTimestamp(range: TokenRange): Timestamp = {
    rangeMembers.get(range) match {
      case Some(member) if cluster.isLocalNode(member.node) => {
        // Local master: lookup consistent timestamp from member recorder
        val timestamp = for {
          recorder <- recorders.get(member.token)
          consistentTimestamp <- recorder.currentConsistentTimestamp
        } yield consistentTimestamp
        timestamp.getOrElse(Long.MinValue)
      }
      case Some(member) if consistencyStates.get(member.token) == Some(MemberConsistencyState.Ok) => {
        // Slave replica: assume everything is consistent if member state is consistent
        Long.MaxValue
      }
      case None => {
        // No member found for the specified range, verify if this is a sub-range of a local master service member.
        val timestamp = recorders.values.find {
          recorder => recorder.member.ranges.exists(recorderRange =>
            recorderRange.contains(range.start) && recorderRange.contains(range.end))
        }.flatMap(_.currentConsistentTimestamp)
        timestamp.getOrElse(Long.MinValue)
      }
      case _ => {
        // The specified range is not a range or sub-range of a master or a slave replica
        Long.MinValue
      }
    }
  }

  /**
   * Manage consistency state and service member state. This method is called when the cluster manager try to change a
   * service member status. The consistency manager upvote or downvote the service member status transition depending
   * on its internal consistency state.
   */
  private def handleStatusTransitionAttemptEvent(event: StatusTransitionAttemptEvent) {
    event.to match {
      case MemberStatus.Down => {
        // Trying to transition the service member Down.
        // When Leaving, allows the transition only if the service member is not the master of a live
        // replication session. Allow transitions from any other status.
        val liveSessions = masterReplicationSessionManager.sessions.filter(_.mode == ReplicationMode.Live).toList
        val canTransition = liveSessions.isEmpty || event.from != MemberStatus.Leaving
        def currentState = if (canTransition) None else consistencyStates.get(event.member.token)
        info("StatusTransitionAttemptEvent: status=Down, prevState={}, newState=None, member={}, live replications={}",
          currentState, event.member, liveSessions.map(_.member))
        if (canTransition) {
          // Reset the service member consistency when transitioning
          metrics.consistencyNone.mark()
          updateMemberConsistencyState(event.member, newState = None)
        }
        event.vote(pass = canTransition)
      }
      case MemberStatus.Joining => {
        // Trying to transition the service member to joining. Initiate consistency recovery if necessary.
        this.synchronized {
          consistencyStates.get(event.member.token) match {
            case None | Some(MemberConsistencyState.Error) => {
              // Joining when service member is not consistent or in process to become consistent! Perform consistency
              // validation and try to restore service member consistency
              val member = ResolvedServiceMember(service, event.member)
              metrics.consistencyRecovering.mark()
              info("StatusTransitionAttemptEvent: status=Joining, prevState=None|Error, newState=Recovering, member={}",
                member)
              updateMemberConsistencyState(event.member, Some(MemberConsistencyState.Recovering))
              restoreMemberConsistency(member, isMaster = true, onSuccess = {
                metrics.consistencyOk.mark()
                info("Local master restoreMemberConsistency: onSuccess {}", member)
                updateMemberConsistencyState(event.member, Some(MemberConsistencyState.Ok))
              }, onError = {
                info("Local master restoreMemberConsistency: onError {}", member)
                metrics.consistencyError.mark()
                updateMemberConsistencyState(event.member, Some(MemberConsistencyState.Error))
              })
              event.vote(pass = false)
            }
            case Some(MemberConsistencyState.Ok) => {
              // Already consistent.
              metrics.consistencyOk.mark()
              info("StatusTransitionAttemptEvent: status=Joining, state=Ok, member={}", event.member)
              event.vote(pass = true)
            }
            case Some(MemberConsistencyState.Recovering) => {
              // Already recovering.
              metrics.consistencyRecovering.mark()
              info("StatusTransitionAttemptEvent: status=Joining, state=Recovering, member={}", event.member)
              event.vote(pass = false)
            }
          }
        }
      }
      case MemberStatus.Up => {
        // Trying to transition the service member Up. Ensure service member is consistent before allowing it to go up.
        this.synchronized {
          consistencyStates.get(event.member.token) match {
            case Some(MemberConsistencyState.Ok) => {
              // Service member is consistent, let member status goes up!
              info("StatusTransitionAttemptEvent: status=Up, state=Ok, member={}", event.member)
              event.vote(pass = true)
            }
            case state => {
              // Service member not consistent yet. Do not allow service member status going Up.
              info("StatusTransitionAttemptEvent: status=Up, state={}, member={}", state, event.member)
              event.vote(pass = false)
            }
          }
        }
      }
      case MemberStatus.Leaving => //no vote for other states
    }
  }

  /**
   * Manage transaction recorder on local service member status changes. Setup a member recorder when the status goes
   * up and remove it when the status goes down. If the service member become inconsistent, change the consistency
   * state to Error and try to put the service member status down.
   */
  private def handleLocalServiceMemberStatusTransitionEvent(event: StatusTransitionEvent) {
    event.to match {
      case MemberStatus.Up => {
        // Initialize transaction recorder for local service member going up
        this.synchronized {
          info("Iniatialize transaction recorders for {}", event.member)
          val txLog = if (txLogEnabled) {
            new FileTransactionLog(service.name, event.member.token, txLogDir, txLogRolloverSize,
              serializer = Some(serializer))
          } else {
            NullTransactionLog
          }
          val member = ResolvedServiceMember(service, event.member)
          val recorder = new TransactionRecorder(member, txLog,
            consistencyDelay = timestampGenerator.responseTimeout + timestampTimeoutExtraDelay,
            consistencyTimeout = math.max(service.responseTimeout + 2000, 15000),
            commitFrequency = txLogCommitFrequency, onConsistencyError = {
              metrics.consistencyError.mark()
              info("onConsistencyError: status={}, prevState={}, newState=Error, member={}", event.member.status,
                consistencyStates.get(member.token), member)
              updateMemberConsistencyState(event.member, Some(MemberConsistencyState.Error))
              cluster.clusterManager.trySetServiceMemberStatusDown(service, event.member)
            })
          recorders += (member.token -> recorder)
          recorder.start()
        }

        // Open a replication session for all service members the local node is a slave
        service.members.withFilter(member =>
          member.status == MemberStatus.Up && isSlaveReplicaOf(member)).foreach {
          SlaveReplicationManagerActor ! OpenSession(_, ReplicationMode.Bootstrap)
        }
      }
      case MemberStatus.Down => {
        this.synchronized {
          // Close all master replication sessions for the member
          masterReplicationSessionManager.terminateMemberSessions(ResolvedServiceMember(service, event.member))

          // Remove transaction recorder for all other cases
          info("Remove transaction recorders for {}", event.member)
          val recorder = recorders.get(event.member.token)
          recorders -= event.member.token
          recorder.foreach(_.kill())
        }
      }
      case MemberStatus.Joining | MemberStatus.Leaving => // Nothing to do
    }
  }

  /**
   * Manage replication sessions on remote service member status change. If local service member is an eligible
   * slave replica of a remote service member going up, open a replication session to start receiving
   * updates from the master.
   */
  private def handleRemoteServiceMemberStatusTransitionEvent(event: StatusTransitionEvent) {
    event.to match {
      case MemberStatus.Up => {
        // Try to open a replication session if we are a slave replica of the service member that just went Up
        if (isSlaveReplicaOf(event.member)) {
          SlaveReplicationManagerActor ! OpenSession(event.member, ReplicationMode.Bootstrap)
        }
      }
      case MemberStatus.Leaving => // Do nothing, must not close session to let drain open replication sessions
      case _ => {
        // Close all slave replication sessions if the remote member status is not Up or Leaving.
        SlaveReplicationManagerActor ! CloseSession(event.member)
      }
    }
  }

  /**
   * This method evaluates if the local node is a slave replica of the specified master serviceMember
   */
  private def isSlaveReplicaOf(member: ServiceMember): Boolean = {
    resolver.resolve(service, member.token).selectedReplicas match {
      case Seq(source, replicas@_*) if replicas.exists(r => cluster.isLocalNode(r.node)) => true
      case _ => false
    }
  }

  /**
   * Update the specified member consistency state. The update is synchronized but the notify call is not.
   */
  private def updateMemberConsistencyState(member: ServiceMember, newState: Option[MemberConsistencyState],
                                           triggerEvent: Boolean = true): Option[ConsistencyStateTransitionEvent] = {
    val prevState = this.synchronized {
      val prevState = consistencyStates.get(member.token)
      newState match {
        case Some(state) => consistencyStates += (member.token -> state)
        case None => consistencyStates -= member.token
      }
      prevState
    }

    if (triggerEvent && prevState != newState) {
      val event = ConsistencyStateTransitionEvent(member, prevState, newState)
      notifyObservers(event)
      Some(event)
    } else None
  }


  private def requiresConsistency(message: Message) = {
    message.serviceName == service.name && service.requiresConsistency(message)
  }

  private def getRecorderFromMessage(message: Message): Option[TransactionRecorder] = {
    for {
      member <- service.resolveMembers(message.token, 1).find(member => cluster.isLocalNode(member.node))
      recorder <- recorders.get(member.token)
    } yield recorder
  }

  override def handleIncoming(action: Action, message: InMessage, next: () => Unit) {
    message.function match {
      case MessageType.FUNCTION_CALL if requiresConsistency(message) => {
        message.method match {
          case ActionMethod.GET => executeConsistentIncomingReadRequest(message, next)
          case _ => executeConsistentIncomingWriteRequest(message, next)
        }
      }
      case _ => {
        next()
      }
    }
  }

  private def executeConsistentIncomingReadRequest(req: InMessage, next: () => Unit) {
    lastWriteTimestamp.get match {
      case timestamp@Some(_) => {
        req.timestamp = timestamp
        next()
      }
      case None => {
        fetchTimestampAndExecuteNext(req, next)
      }
    }
  }

  private def executeConsistentIncomingWriteRequest(req: InMessage, next: () => Unit) {
    fetchTimestampAndExecuteNext(req, () => {
      getRecorderFromMessage(req) match {
        case Some(recorder) => {
          try {
            recorder.appendMessage(req)
            next()
          } catch {
            case e: Exception => {
              req.replyWithError(e)
            }
          }
        }
        case None => {
          metrics.inRequestServiceDown.mark()
          warn("No transaction recorder found for request (token {}, message={}).", req.token, req)
          req.replyWithError(new UnavailableException)
        }
      }
    })
  }

  private def fetchTimestampAndExecuteNext(req: InMessage, next: () => Unit) {
    timestampGenerator.fetchTimestamps(req.serviceName, (timestamps: Seq[Timestamp], optException) => {
      try {
        if (optException.isDefined) {
          info("Exception while fetching timestamps.", optException.get.toString)
          throw optException.get
        }
        val timestamp = timestamps(0)
        lastWriteTimestamp.update(Some(timestamp))
        req.timestamp = Some(timestamp)
        next()
      } catch {
        case e: Exception =>
          req.replyWithError(e)
      }
    }, 1, req.token)
  }

  override def handleOutgoing(action: Action, message: OutMessage, next: () => Unit) {
    message.function match {
      case MessageType.FUNCTION_RESPONSE if requiresConsistency(message) => {
        //CASE: send response to the original sender node. Does not uses resolver to populate destination nodes.
        message.method match {
          case ActionMethod.GET => executeConsistentOutgoingReadResponse(message, next)
          case _ => executeConsistentOutgoingWriteResponse(message, next)
        }
      }
      case MessageType.FUNCTION_CALL if requiresConsistency(message) => {
        message.method match {
          case ActionMethod.GET => executeConsistentOutgoingReadRequest(message, next)
          case _ => executeConsistentOutgoingWriteRequest(action, message, next)
        }
      }
      case _ => {
        // Other outgoing messages (e.g. replication session messages). The message should only be sent
        // to a single destination if it doesn't require consistency.
        message.destination.deselectAllReplicasButFirst()
        message.destination.selectedReplicas.isEmpty match {
          case true => simulateUnavailableResponse(action, message)
          case false => next()
        }
      }
    }
  }

  /**
   *  This method will send an error response to a message with an unresolvable destination.
   *  The response is sent to itself by calling handleIncoming on the current edge server (instead of
   *  sending it through the nrv network protocol
   */
  private def simulateUnavailableResponse(action: Action, message: OutMessage) {
    val response = new InMessage()
    message.copyTo(response)
    response.error = Some(new UnavailableException)
    response.function = MessageType.FUNCTION_RESPONSE
    service.findAction(message.path, message.method) match {
      case Some(action: Action) => action.callIncomingHandlers(response)
      case _ =>
    }
  }

  private def executeConsistentOutgoingReadResponse(res: OutMessage, next: () => Unit) {
    next()
  }

  private def executeConsistentOutgoingWriteResponse(res: OutMessage, next: () => Unit) {
    getRecorderFromMessage(res) match {
      case Some(recorder) => {
        try {
          recorder.appendMessage(res)
        } catch {
          case e: Exception => {
            res.error = Some(e)
          }
        }
      }
      case None => {
        metrics.outResponseServiceDown.mark()
        warn("No transaction recorder found for response (token {}, message={}).", res.token, res)
        res.error = Some(new UnavailableException)
      }
    }

    next()
  }

  def executeConsistentOutgoingReadRequest(message: OutMessage, next: () => Unit) {
    // Select the first available destination. Read messages are handled by the master if available but any replicas
    // could also handle it.
    message.destination.deselectAllReplicasButOne()
    next()
  }

  def executeConsistentOutgoingWriteRequest(action: Action, message: OutMessage, next: () => Unit) {
    // Only the master (first resolved node) may handle write messages
    message.destination.replicas match {
      case master :: _ if master.selected => {
        message.destination.deselectAllReplicasButFirst()
        next()
      }
      case _ => simulateUnavailableResponse(action, message) //case: master down
    }
  }

  private def restoreMemberConsistency(member: ResolvedServiceMember, isMaster: Boolean, onSuccess: => Unit, onError: => Unit) {
    try {
      // TODO: Use Future
      val recovery = new ConsistencyRecovery(txLogDir, service, Some(serializer))
      val finalLogIndex = recovery.restoreMemberConsistency(member, onError)
      finalLogIndex.flatMap(_.consistentTimestamp) match {
        case Some(lastLogTimestamp) => {
          // Ensure that transaction log and storage have the same final transaction timestamp
          service.getLastTimestamp(member.ranges) match {
            case Some(lastStoreTimestamp) if lastStoreTimestamp == lastLogTimestamp => {
              info("The service member transaction log and store are consistent {}", member)
              onSuccess
            }
            case Some(lastStoreTimestamp) if lastStoreTimestamp < lastLogTimestamp => {
              info("Possible transaction log and storage inconsistency. Falling back to slower committed timestamp verification (store={}, log={}) {}",
                lastLogTimestamp, lastLogTimestamp, member)
              val txLog = new FileTransactionLog(service.name, member.token, txLogDir, txLogRolloverSize,
                serializer = Some(serializer))
              txLog.lastSuccessfulTimestamp(lastStoreTimestamp) match {
                case Some(lastCommitedLogTimestamp) if lastCommitedLogTimestamp == lastStoreTimestamp => {
                  info("The service member transaction log and store are consistent {}", member)
                  onSuccess
                }
                case Some(lastCommittedLogTimestamp) => {
                  error("Last transaction log committed timestamp and storage last timestamp are different! (store={}, log={}) {}",
                    lastLogTimestamp, lastCommittedLogTimestamp, member)
                  onError
                }
                case None => {
                  error("Cannot find last transaction log committed timestamp! (store={}) {}",
                    lastStoreTimestamp, member)
                  onError
                }
              }
            }
            case Some(lastStoreTimestamp) => {
              error("Transaction log and storage last timestamps are different! (store={}, log={}) {}",
                lastStoreTimestamp, lastLogTimestamp, member)
              onError
            }
            case None => {
              error("Service member is inconsistent. Transaction log exists but storage is empty! (log={}) {}",
                lastLogTimestamp, member)
              onError
            }
          }
        }
        case None if isMaster => {
          // No transaction log and master service member, assume the store is consistent
          info("The master service member has no transaction log and is assumed to be consistent {}", member)
          onSuccess
        }
        case None => {
          // No transaction log and slave replica, assume the store is consistent
          error("Service member is inconsistent. The slave service member has no transaction log {}", member)
          onError
        }
      }
    } catch {
      case e: Exception => {
        error("Got an exception during the service member recovery! {}", member, e)
        onError
      }
    }
  }

  private object SlaveReplicationManagerProtocol {

    case class OpenSession(member: ServiceMember, mode: ReplicationMode)

    case class CloseSession(member: ServiceMember)

    object Kill

  }

  /**
   * Manage local slave replication sessions. The usage of actor ensure that no concurrent open session call is
   * done in parallel for the same service member.
   */
  private object SlaveReplicationManagerActor extends Actor {

    import SlaveReplicationManagerProtocol._

    private def openSession(member: ServiceMember, mode: ReplicationMode) {
      info("Local replica open a replication session to {}", member)

      // No recovery if already has a session to prevent transaction log corruption
      val resolvedMember = ResolvedServiceMember(service, member)
      if (!slaveReplicationSessionManager.sessions.exists(_.member == resolvedMember)) {
        restoreMemberConsistency(resolvedMember, isMaster = false, onSuccess = {
          metrics.consistencyOk.mark()
          info("Local replica restoreMemberConsistency: onSuccess {}", member)
          val openSessionDelay = mode match {
            case ReplicationMode.Bootstrap => replicationOpenSessionDelay
            case ReplicationMode.Live => 0
          }
          val txLog = new FileTransactionLog(service.name, member.token, txLogDir, txLogRolloverSize,
            serializer = Some(serializer))
          slaveReplicationSessionManager.openSession(resolvedMember, txLog, openSessionDelay,
            masterOpenSessionAction, masterCloseSessionAction, mode,
            onSessionEnd = (error) => {
              info("Replication session terminated {}. {}", resolvedMember, error)
              updateMemberConsistencyState(member, newState = error.map(_ => MemberConsistencyState.Error))

              // Renew the replication session if the master replica is up
              txLog.commit()
              txLog.close()
              if (member.status == MemberStatus.Up) {
                // If session ends gracefully, assumes we can switch to live replication
                val newMode = if (error.isDefined) ReplicationMode.Bootstrap else ReplicationMode.Live
                SlaveReplicationManagerActor ! OpenSession(member, newMode)
              }
            })
          updateMemberConsistencyState(member, Some(MemberConsistencyState.Ok))
        }, onError = {
          metrics.consistencyError.mark()
          info("Local replica restoreMemberConsistency: onError {}", member)
          updateMemberConsistencyState(member, Some(MemberConsistencyState.Error))
        })
      }
    }

    def act() {
      loop {
        react {
          case OpenSession(member, mode) => {
            try {
              openSession(member, mode)
            } catch {
              case e: Exception => {
                warn("Error processing open session for {}", member, e)
              }
            }
          }
          case CloseSession(member) => {
            try {
              slaveReplicationSessionManager.closeSession(ResolvedServiceMember(service, member))
            } catch {
              case e: Exception => {
                warn("Error processing close session for {}", member, e)
              }
            }
          }
          case Kill => {
            try {
              exit()
            } finally {
              reply(true)
            }
          }
        }
      }
    }
  }

  private class Metrics(scope: String) extends Instrumented {
    lazy val inRequestServiceDown = new Meter(metrics.metricsRegistry.newMeter(classOf[ConsistencyMasterSlave],
      "in-request-service-down", scope, "in-request-service-down", TimeUnit.SECONDS))
    lazy val outResponseServiceDown = new Meter(metrics.metricsRegistry.newMeter(classOf[ConsistencyMasterSlave],
      "out-response-service-down", scope, "out-response-service-down", TimeUnit.SECONDS))
    lazy val consistencyNone = new Meter(metrics.metricsRegistry.newMeter(classOf[ConsistencyMasterSlave],
      "consistency-none", scope, "consistency-none", TimeUnit.SECONDS))
    lazy val consistencyOk = new Meter(metrics.metricsRegistry.newMeter(classOf[ConsistencyMasterSlave],
      "consistency-ok", scope, "consistency-ok", TimeUnit.SECONDS))
    lazy val consistencyRecovering = new Meter(metrics.metricsRegistry.newMeter(classOf[ConsistencyMasterSlave],
      "consistency-recovering", scope, "consistency-recovering", TimeUnit.SECONDS))
    lazy val consistencyError = new Meter(metrics.metricsRegistry.newMeter(classOf[ConsistencyMasterSlave],
      "consistency-error", scope, "consistency-error", TimeUnit.SECONDS))

    private val consistencyOkCountGauge = metrics.metricsRegistry.newGauge(classOf[ConsistencyMasterSlave],
      "consistency-ok-count", scope, new Gauge[Long] {
        def value = consistencyStates.values.count(_ == MemberConsistencyState.Ok)
      })
    private val consistencyErrorCountGauge = metrics.metricsRegistry.newGauge(classOf[ConsistencyMasterSlave],
      "consistency-error-count", scope, new Gauge[Long] {
        def value = consistencyStates.values.count(_ == MemberConsistencyState.Error)
      })
    private val consistencyRecoveringCountGauge = metrics.metricsRegistry.newGauge(classOf[ConsistencyMasterSlave],
      "consistency-recovering-count", scope, new Gauge[Long] {
        def value = consistencyStates.values.count(_ == MemberConsistencyState.Recovering)
      })

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
