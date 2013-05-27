package com.wajam.nrv.consistency

import com.wajam.nrv.service._
import com.wajam.nrv.data.{OutMessage, Message, MessageType, InMessage}
import com.wajam.nrv.utils.timestamp.{Timestamp, TimestampGenerator}
import com.yammer.metrics.scala.{Meter, Instrumented}
import com.wajam.nrv.utils.Event
import com.wajam.nrv.service.StatusTransitionEvent
import persistence.{LogRecordSerializer, NullTransactionLog, FileTransactionLog}
import java.util.concurrent.TimeUnit
import com.yammer.metrics.core.Gauge
import com.wajam.nrv.UnavailableException
import com.wajam.nrv.consistency.replication.{ReplicationMode, ReplicationSubscriber, ReplicationPublisher, ReplicationParam}

/**
 * Consistency manager for consistent master/slave replication of the binded storage service. The mutation messages are
 * recorded in a transaction log per service member and replicated to slave replicas.
 *
 * TODO: more about transfering the data from log or from store. More about replicas selection.
 *
 * The consistency manager ensure that the storage and the transaction log are always consistent. A service member is
 * only allowed to goes Up if the store and the transaction log are consistent. In case of inconsistency during the
 * service member storage lifetime, the service member status is set to Down. The consistency manager tries to perform
 * the necessary recovery while the service member tries to go up.
 *
 * ASSUMPTIONS:
 * - The messages for a given token are sequenced before reaching the consistency manager.
 * - Messages timestamps are unique in the whole cluster and also sequenced per message token.
 *
 * IMPORTANT NOTES:
 * - This class is still a work in progress.
 * - Temporary extends ConsistencyOne until real master/slave replicas selection implementation.
 * - Support binding to a single service. The service must extends ConsistentStore.
 */
class ConsistencyMasterSlave(val timestampGenerator: TimestampGenerator, txLogDir: String, txLogEnabled: Boolean,
                             txLogRolloverSize: Int = 50000000, txLogCommitFrequency: Int = 5000,
                             replicationTps: Int = 50, replicationWindowSize: Int = 20,
                             replicationSubscriptionIdleTimeout: Long = 30000L,
                             replicationResolver: Option[Resolver] = None)
  extends ConsistencyOne {

  private val lastWriteTimestamp = new AtomicTimestamp(AtomicTimestamp.updateIfGreater, None)

  @volatile // updates are synchronized but lookups are not
  private var recorders: Map[Long, TransactionRecorder] = Map()

  private var consistencyStates: Map[Long, MemberConsistencyState] = Map()

  private var metrics: Metrics = null

  def service: Service with ConsistentStore = bindedServices.head.asInstanceOf[Service with ConsistentStore]

  def resolver = replicationResolver.getOrElse(service.resolver)

  def serializer = new LogRecordSerializer(service.nrvCodec)

  // Mapping between token ranges and service member to speedup consistent timestamp lookup function passed to
  // the consistent storage.
  private var rangeMembers: Map[TokenRange, ServiceMember] = Map()

  private def updateRangeMemberCache() {
    rangeMembers = service.members.flatMap(member => service.getMemberTokenRanges(member).map((_, member))).toMap
  }

  // Replication subscriber action
  private lazy val replicationSubscriber = new ReplicationSubscriber(service, service,
    replicationSubscriptionIdleTimeout, txLogCommitFrequency)
  private lazy val publishAction = new Action("/replication/publish/:" + ReplicationParam.SubscriptionId,
    replicationSubscriber.handlePublishMessage(_), ActionMethod.POST)

  // Replication publisher actions
  private lazy val replicationPublisher: ReplicationPublisher = {
    def getTransactionLog(member: ResolvedServiceMember) = recorders.get(member.token) match {
      case Some(recorder) => recorder.txLog
      case None => NullTransactionLog
    }

    def getMemberCurrentConsistentTimestamp(member: ResolvedServiceMember) = recorders.get(member.token) match {
      case Some(recorder) => recorder.currentConsistentTimestamp
      case None => None
    }

    new ReplicationPublisher(service, service, getTransactionLog, getMemberCurrentConsistentTimestamp,
      publishAction = publishAction, publishTps = replicationTps, publishWindowSize = replicationWindowSize,
      maxIdleDurationInMs = replicationSubscriptionIdleTimeout)
  }
  private lazy val subscribeAction = new Action("/replication/subscribe/:" + ReplicationParam.Token,
    replicationPublisher.handleSubscribeMessage(_), ActionMethod.POST)
  private lazy val unsubscribeAction = new Action("/replication/unsubscribe/:" + ReplicationParam.Token,
    replicationPublisher.handleUnsubscribeMessage(_), ActionMethod.POST)

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
    super.start()

    // TODO: update cache when new members are added/removed
    updateRangeMemberCache()

    publishAction.applySupport(resolver = Some(new Resolver(tokenExtractor = Resolver.TOKEN_RANDOM())),
      nrvCodec = Some(serializer.messageCodec))
    subscribeAction.applySupport(resolver = replicationResolver, nrvCodec = Some(serializer.messageCodec))
    unsubscribeAction.applySupport(resolver = replicationResolver, nrvCodec = Some(serializer.messageCodec))
    publishAction.start()
    subscribeAction.start()
    unsubscribeAction.start()

    replicationPublisher.start()
    replicationSubscriber.start()
  }

  override def stop() {
    super.stop()

    replicationSubscriber.stop()
    replicationPublisher.stop()

    publishAction.stop()
    subscribeAction.stop()
    unsubscribeAction.stop()
  }

  override def bindService(service: Service) {
    require(service.isInstanceOf[ConsistentStore],
      "Consistent service must be type of %s but is %s".format(classOf[ConsistentStore], service.getClass))
    require(bindedServices.size == 0, "Cannot bind to multiple services. Already bound to %s".format(bindedServices.head))

    super.bindService(service)

    metrics = new Metrics(service.name.replace(".", "-"))

    // Setup the current consistent timestamp lookup function to the consistent storage
    info("Setup consistent timestamp lookup for service", service.name)
    this.service.setCurrentConsistentTimestamp((range) => {
      rangeMembers.get(range) match {
        // Local master: lookup consistent timestamp from member recorder
        case Some(member) if cluster.isLocalNode(member.node) => {
          val timestamp = for {
            recorder <- recorders.get(member.token)
            consistentTimestamp <- recorder.currentConsistentTimestamp
          } yield consistentTimestamp
          timestamp.getOrElse(Long.MinValue)
        }
        // Slave replica: assume everything is consistent if member state is consistent
        case Some(member) if consistencyStates.get(member.token) == Some(MemberConsistencyState.Ok) => Long.MaxValue
        // Not a master or slave replica
        case _ => Long.MinValue
      }
    })

    // Register replication subscribe/publish actions
    service.registerAction(publishAction)
    service.registerAction(subscribeAction)
    service.registerAction(unsubscribeAction)
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
      case _ => // Ignore unsupported events
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
        // Trying to transition the service member Down. Reset the service member consistency.
        metrics.consistencyNone.mark()
        info("StatusTransitionAttemptEvent: status=Down, prevState={}, newState=None, member={}",
          consistencyStates.get(event.member.token), event.member)
        updateMemberConsistencyState(event.member, newState = None)
        event.vote(pass = true)
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
              restoreMemberConsistency(member, onSuccess = {
                metrics.consistencyOk.mark()
                info("Local master restoreMemberConsistency: onSuccess {}", member)
                updateMemberConsistencyState(event.member, Some(MemberConsistencyState.Ok))
              }, onError = {
                info("Local master restoreMemberConsistency: onError {}", member)
                metrics.consistencyError.mark()
                updateMemberConsistencyState(event.member, Some(MemberConsistencyState.Error))
              })
            }
            case Some(MemberConsistencyState.Ok) => {
              // Already consistent.
              metrics.consistencyOk.mark()
              info("StatusTransitionAttemptEvent: status=Joining, state=Ok, member={}", event.member)
            }
            case Some(MemberConsistencyState.Recovering) => {
              // Already recovering.
              metrics.consistencyRecovering.mark()
              info("StatusTransitionAttemptEvent: status=Joining, state=Recovering, member={}", event.member)
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
        // Iniatialize transaction recorder for local service member going up
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
            consistencyDelay = timestampGenerator.responseTimeout + 1000,
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

        // Subscribe to replication source already up for which we are a slave replica
        service.members.withFilter(member =>
          member.status == MemberStatus.Up && isSlaveReplicaOf(member)).foreach(subscribe(_, ReplicationMode.Store))
      }
      case MemberStatus.Down => {
        this.synchronized {
          // Cancel all master replication subscriptions for the member
          replicationPublisher.terminateSubscriptions(ResolvedServiceMember(service, event.member))

          // Remove transaction recorder for all other cases
          info("Remove transaction recorders for {}", event.member)
          val recorder = recorders.get(event.member.token)
          recorders -= event.member.token
          recorder.foreach(_.kill())
        }
      }
      case MemberStatus.Joining => // Nothing to do
    }
  }

  /**
   * Manage replication subscriptions on remote service member status change. If local service member is an eligible
   * slave replica of a remote service member going up, initiate a replication subscription to start receiving
   * updates from source replica.
   */
  private def handleRemoteServiceMemberStatusTransitionEvent(event: StatusTransitionEvent) {
    event.to match {
      case MemberStatus.Up => {
        // Subscribe to remote source replica if we are a slave replica of the service member that just went Up
        if (isSlaveReplicaOf(event.member)) {
          subscribe(event.member, ReplicationMode.Store)
        }
      }
      case _ =>
        // Unsubscribe slave replication subscriptions if the remote member status is not Up.
        replicationSubscriber.unsubscribe(ResolvedServiceMember(service, event.member))
    }
  }

  private def isSlaveReplicaOf(member: ServiceMember): Boolean = {
    resolver.resolve(service, member.token).selectedReplicas match {
      case Seq(source, replicas@_*) if replicas.exists(r => cluster.isLocalNode(r.node)) => true
      case _ => false
    }
  }

  private def subscribe(member: ServiceMember, mode: ReplicationMode) {
    info("Local replica is subscribing to {}", member)

    val resolvedMember = ResolvedServiceMember(service, member)
    // TODO: no recovery if already has a subscription to prevent transaction log corruption
    restoreMemberConsistency(resolvedMember, onSuccess = {
      // TODO: metric
      info("Local replica restoreMemberConsistency: onSuccess {}", member)
      // TODO: configurable subscribe delay
      // TODO: no delay on live replication mode
      val txLog = new FileTransactionLog(service.name, member.token, txLogDir, txLogRolloverSize,
        serializer = Some(serializer))
      replicationSubscriber.subscribe(resolvedMember, txLog, 5000, subscribeAction, unsubscribeAction, mode,
        onSubscriptionEnd = (error) => {
          info("Replication subscription terminated {}. {}", resolvedMember, error)
          updateMemberConsistencyState(member, newState = error.map(_ => MemberConsistencyState.Error))

          // Renew the replication subscription if the master replica is up
          txLog.commit()
          txLog.close()
          if (member.status == MemberStatus.Up) {
            // If subscription ends gracefully, assumes we can switch to live replication
            val newMode = if (error.isDefined) ReplicationMode.Store else ReplicationMode.Live
            subscribe(member, newMode)
          }
        })
      updateMemberConsistencyState(member, Some(MemberConsistencyState.Ok))
    }, onError = {
      // TODO: metric
      info("Local replica restoreMemberConsistency: onError {}", member)
      updateMemberConsistencyState(member, Some(MemberConsistencyState.Error))
    })
  }

  /**
   * Update the specified member consistency state. The update is synchromized but the notify call is not.
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
    lastWriteTimestamp.get match {
      case timestamp @ Some(_) => {
        req.timestamp = timestamp
        next()
      }
      case None => {
        fetchTimestampAndExecuteNext(req, next)
      }
    }
  }

  private def executeConsistentWriteRequest(req: InMessage, next: Unit => Unit) {
    fetchTimestampAndExecuteNext(req, _ => {
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

  private def fetchTimestampAndExecuteNext(req: InMessage, next: Unit => Unit) {
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

  private def restoreMemberConsistency(member: ResolvedServiceMember, onSuccess: => Unit, onError: => Unit) {
    try {
      // TODO: Use Future
      val recovery = new ConsistencyRecovery(txLogDir, service, Some(serializer))
      val finalLogIndex = recovery.restoreMemberConsistency(member, onError)
      finalLogIndex.map(_.consistentTimestamp) match {
        case Some(lastLogTimestamp) => {
          // Ensure that transaction log and storage have the same final transaction timestamp
          val lastStoreTimestamp = service.getLastTimestamp(member.ranges)
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
