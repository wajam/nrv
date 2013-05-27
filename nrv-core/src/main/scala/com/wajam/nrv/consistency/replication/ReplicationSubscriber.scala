package com.wajam.nrv.consistency.replication

import com.wajam.nrv.service.{TokenRange, Action, Service}
import com.wajam.nrv.consistency.{ConsistencyException, ResolvedServiceMember, ConsistentStore}
import com.wajam.nrv.data.{MValue, InMessage, Message}
import com.wajam.nrv.utils.timestamp.Timestamp
import scala.actors.Actor
import com.wajam.nrv.utils.{CurrentTime, TimestampIdGenerator, IdGenerator, Scheduler}
import collection.immutable.TreeSet
import com.wajam.nrv.consistency.persistence.TransactionLog
import ReplicationParam._
import com.wajam.nrv.consistency.persistence.LogRecord.{Index, Response, Request}
import com.wajam.nrv.consistency.persistence.LogRecord.Response.Success
import annotation.tailrec
import com.wajam.nrv.{TimeoutException, Logging}
import java.util.{TimerTask, Timer}
import com.yammer.metrics.scala.Instrumented
import util.Random

/**
 * Manage all replication subscriptions the local service is subscribing. Only one replication subscription per
 * service member is allowed.
 */
class ReplicationSubscriber(service: Service, store: ConsistentStore, maxIdleDurationInMs: Long, commitFrequency: Int)
  extends CurrentTime with Logging with Instrumented {

  private val manager = new SubscriptionManagerActor

  private val serviceScope = service.name.replace(".", "-")
  private val subscriptions = metrics.gauge("subscriptions", serviceScope) {
    manager.subscriptionsCount
  }

  def start() {
    manager.start()
  }

  def stop() {
    manager !? SubscriptionManagerProtocol.Kill
  }

  /**
   * Subscribe can be delayed i.e. the subscribe call to the replication source is done after
   * a specified amount of time. This class is keeping track of the pending subscriptions (i.e. delayed or awaiting
   * replication source response). All new subscibe calls for a given member are silently ignored if a subscription
   * (pending or active) already exists for the member.
   */
  def subscribe(member: ResolvedServiceMember, txLog: TransactionLog, delay: Long, subscribeAction: Action,
                unsubscribeAction: Action, mode: ReplicationMode, onSubscriptionEnd: Option[Exception] => Unit) {
    manager ! SubscriptionManagerProtocol.Subscribe(member, txLog, subscribeAction, unsubscribeAction, mode,
      onSubscriptionEnd, delay)
  }

  def unsubscribe(member: ResolvedServiceMember) {
    manager ! SubscriptionManagerProtocol.Unsubscribe(member)
  }

  /**
   * Process replication publish message received from the replication source.
   */
  def handlePublishMessage(message: InMessage) {
    manager ! SubscriptionManagerProtocol.Publish(message)
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

  object SubscriptionManagerProtocol {

    case class Subscribe(member: ResolvedServiceMember, txLog: TransactionLog, subscribeAction: Action,
                         unsubscribeAction: Action, mode: ReplicationMode,
                         onSubscriptionEnd: Option[Exception] => Unit, delay: Long = 0)

    // TODO: Either[Exception, Message]???
    case class SubscribeResponse(subscribe: Subscribe, response: Message, optException: Option[Exception])

    case class Unsubscribe(member: ResolvedServiceMember)

    case class Publish(message: InMessage)

    case class TerminateSubscription(subscription: SubscriptionActor, error: Option[Exception] = None)

    object Kill

  }

  class SubscriptionManagerActor extends Actor {

    private lazy val subscribeMeter = metrics.meter("subscribe", "subscribe", serviceScope)
    private lazy val subscribeIgnoreMeter = metrics.meter("subscribe-ignore", "subscribe-ignore", serviceScope)
    private lazy val subscribeOkMeter = metrics.meter("subscribe-ok", "subscribe-ok", serviceScope)
    private lazy val subscribeErrorMeter = metrics.meter("subscribe-error", "subscribe-error", serviceScope)

    private lazy val unsubscribeMeter = metrics.meter("unsubscribe", "subscribe", serviceScope)
    private lazy val unsubscribeErrorMeter = metrics.meter("unsubscribe-error", "subscribe-error", serviceScope)

    private lazy val publishMeter = metrics.meter("publish", "publish", serviceScope)
    private lazy val publishIgnoreMeter = metrics.meter("publish-ignore", "publish-ignore", serviceScope)
    private lazy val publishErrorMeter = metrics.meter("publish-error", "publish-error", serviceScope)

    private lazy val terminateErrorMeter = metrics.meter("terminate-error", "terminate-error", serviceScope)

    import SubscriptionManagerProtocol._

    private case class PendingSubscription(subscribe: Subscribe)

    private val timer = new Timer("SubscriptionManagerActor-Timer")

    private var subscriptions: Map[ResolvedServiceMember, Either[PendingSubscription, SubscriptionActor]] = Map()

    def subscriptionsCount = subscriptions.size

    private var subscriptionGaugeCache: Map[ResolvedServiceMember, SubscriptionCurrentTimestampGauge] = Map()

    private def getSubscriptionGauge(member: ResolvedServiceMember) = subscriptionGaugeCache.getOrElse(member, {
      val gauge = new SubscriptionCurrentTimestampGauge(member)
      subscriptionGaugeCache += (member -> gauge)
      gauge
    })

    private def terminateSubscription(subscription: SubscriptionActor, error: Option[Exception]) {
      subscriptions -= (subscription.member)
      subscription ! SubscriptionProtocol.Kill

      sendUnsubscribe(subscription.subscribe, subscription.subId)
    }

    private def sendUnsubscribe(subscription: Subscribe, subscriptionId: String) {
      def handleUnsubscribeResponse(reponse: InMessage, error: Option[Exception]) {
        error match {
          case Some(e) => info("Unsubscribe reponse error {} for {}: ", subscriptionId, subscription.member, e)
          case None => debug("Unsubscribe reponse {} for {}", subscriptionId, subscription.member)
        }
      }

      info("Send unsubscribe request to source for subscription {}. {}", subscriptionId, subscription.member)
      var params: Map[String, MValue] = Map()
      params += (ReplicationParam.SubscriptionId -> subscriptionId)
      params += (ReplicationParam.Token -> subscription.member.token.toString)
      subscription.unsubscribeAction.call(params, onReply = handleUnsubscribeResponse(_, _))
    }

    def act() {
      loop {
        react {
          case subscribe: Subscribe => {
            try {
              subscribeMeter.mark()
              subscriptions.get(subscribe.member) match {
                case Some(Left(_)) => {
                  subscribeIgnoreMeter.mark()
                  info("Ignore new subscribe request. Already have a pending subscription registered for {}.",
                    subscribe.member)
                }
                case Some(Right(_)) => {
                  subscribeIgnoreMeter.mark()
                  info("Ignore new subscribe request. Already have an active subscription registered for {}.",
                    subscribe.member)
                }
                case None => {
                  info("Registering a new pending subscription for {}", subscribe.member)
                  timer.schedule(new TimerTask {
                    def run() {
                      SubscriptionManagerActor.this ! PendingSubscription(subscribe)
                    }
                  }, subscribe.delay)
                  subscriptions += (subscribe.member -> Left(PendingSubscription(subscribe)))
                }
              }
            } catch {
              case e: Exception => {
                subscribeErrorMeter.mark()
                warn("Error processing subscribe for {}", subscribe.member, e)
                subscriptions -= (subscribe.member)
                subscribe.onSubscriptionEnd(Some(e))
              }
            }
          }
          case PendingSubscription(subscribe) => {
            try {
              subscriptions.get(subscribe.member) match {
                case Some(Left(_)) => {
                  info("Send subscribe request to source for pending subscription. {}", subscribe.member)
                  var params: Map[String, MValue] = Map()
                  params += (ReplicationParam.Token -> subscribe.member.token.toString)
                  subscribe.txLog.getLastLoggedRecord.map(_.consistentTimestamp) match {
                    case Some(Some(lastTimestamp)) => {
                      params += (ReplicationParam.Start -> lastTimestamp.toString)
                    }
                    case _ => {
                      // No records in transaction log. Omit start if the local store is empty.
                      // The replication publisher will send all the transactions from the beginning.
                      // TODO: prevent replication if store not empty and has no transaction log
                    }
                  }
                  params += (ReplicationParam.Mode -> subscribe.mode.toString)
                  subscribe.subscribeAction.call(params,
                    onReply = SubscriptionManagerActor.this ! SubscribeResponse(subscribe, _, _))
                }
                case Some(Right(_)) => {
                  subscribeIgnoreMeter.mark()
                  warn("Do not process pending subscription. Already have an active subscription for {}.",
                    subscribe.member)
                }
                case None => {
                  subscribeIgnoreMeter.mark()
                  info("Do not process pending subscription. No more pending subscription registered for {}.",
                    subscribe.member)
                }
              }
            } catch {
              case e: Exception => {
                subscribeErrorMeter.mark()
                warn("Error processing delayed subscribe for {}", subscribe.member, e)
                subscriptions -= (subscribe.member)
                subscribe.onSubscriptionEnd(Some(e))
              }
            }
          }
          case SubscribeResponse(subscribe, message, exception) => {
            try {
              implicit val response = message

              exception match {
                case Some(e) => {
                  subscribeErrorMeter.mark()
                  warn("Got a subscribe response error for {}: ", subscribe.member, e)
                  subscriptions -= (subscribe.member)
                  subscribe.onSubscriptionEnd(Some(e))
                }
                case None => {
                  val subscriptionId = getParamStringValue(SubscriptionId)
                  val startTimestamp = Timestamp(getParamLongValue(Start))
                  val endTimestamp = getOptionalParamLongValue(End).map(ts => Timestamp(ts))

                  subscriptions.get(subscribe.member) match {
                    case Some(Left(_)) if Some(startTimestamp) == endTimestamp => {
                      subscribeIgnoreMeter.mark()
                      info("Do not activate subscription. " +
                        "Subscription is empty i.e. has identical start/end timestamps) {}: {}",
                        response, subscribe.member)

                      subscriptions -= subscribe.member
                      sendUnsubscribe(subscribe, subscriptionId)
                      subscribe.onSubscriptionEnd(None)
                    }
                    case Some(Left(_)) => {
                      info("Subscribe response {}. Activate subscription {}.", response, subscribe.member)

                      val subscription = new SubscriptionActor(subscriptionId, startTimestamp, endTimestamp, subscribe,
                        getSubscriptionGauge(subscribe.member), new TimestampIdGenerator)
                      subscriptions += (subscribe.member -> Right(subscription))
                      subscription.start()
                      subscribeOkMeter.mark()
                    }
                    case Some(Right(_)) => {
                      subscribeIgnoreMeter.mark()
                      warn("Do not activate subscription. Already have an active subscription for {}.",
                        subscribe.member)

                      sendUnsubscribe(subscribe, subscriptionId)
                    }
                    case None => {
                      subscribeIgnoreMeter.mark()
                      info("Do not activate subscription. No more subscription registered for {}.",
                        subscribe.member)

                      sendUnsubscribe(subscribe, subscriptionId)
                    }
                  }
                }
              }
            } catch {
              case e: Exception => {
                subscribeErrorMeter.mark()
                warn("Error processing subscribe response for {}", subscribe.member, e)
                subscriptions -= (subscribe.member)
                subscribe.onSubscriptionEnd(Some(e))
              }
            }
          }
          case Unsubscribe(member) => {
            try {
              subscriptions.get(member) match {
                case Some(Left(PendingSubscription(subscribe))) => {
                  unsubscribeMeter.mark()
                  info("Unsubscribe. Remove pending subscription. {}", member)
                  subscriptions -= (member)
                }
                case Some(Right(subscription)) => {
                  unsubscribeMeter.mark()
                  info("Unsubscribe. Remove active subscription for. {}", member)
                  terminateSubscription(subscription, None)
                }
                case None => {
                  debug("Unsubscribe. No action taken, no subscription registered for {}.", member)
                }
              }
            } catch {
              case e: Exception => {
                unsubscribeErrorMeter.mark()
                warn("Error processing unsubscribe for {}", member, e)
              }
            }
          }
          case Publish(message) => {
            try {
              val subId = getParamStringValue(SubscriptionId)(message)
              subscriptions.collectFirst({
                case (member, Right(subscription)) if subscription.subId == subId => subscription
              }) match {
                case Some(subscription) => {
                  publishMeter.mark()
                  if (message.hasData) {
                    subscription ! SubscriptionProtocol.Transaction(message)
                  } else {
                    subscription ! SubscriptionProtocol.KeepAlive(message)
                  }
                }
                case None => {
                  publishIgnoreMeter.mark()
                  // Cannot unsubscribe from replication source, do not have access to unsubscribe Action
                }
              }
            } catch {
              case e: Exception => {
                publishErrorMeter.mark()
                warn("Error processing publish {}", message, e)
              }
            }
          }
          case TerminateSubscription(subscriptionActor, error) => {
            try {
              subscriptions.get(subscriptionActor.member) match {
                case Some(Right(subscription)) if subscription.subId == subscriptionActor.subId => {
                  info("Subscription actor {} wants to be terminated. Dutifully perform euthanasia! {}",
                    subscriptionActor.subId, subscriptionActor.member)
                  terminateSubscription(subscriptionActor, error)
                  subscription.subscribe.onSubscriptionEnd(error)
                }
                case _ => // Not subscribed anymore to that subscription. Take no local action.
              }
            } catch {
              case e: Exception => {
                terminateErrorMeter.mark()
                warn("Error processing terminate subscription. {}", subscriptionActor.member, e)
              }
            }
          }
          case Kill => {
            try {
              subscriptions.valuesIterator.foreach({
                case Right(subscription) => subscription !? SubscriptionProtocol.Kill
              })
              subscriptions = Map()
            } catch {
              case e: Exception => {
                warn("Error killing subscription manager ({}). {}", service.name, e)
              }
            } finally {
              reply(true)
            }
          }
        }
      }
    }

  }

  object SubscriptionProtocol {

    trait Publish extends Ordered[Publish] {
      def publishMessage: InMessage

      def sequence: Long

      def compare(that: Publish) = sequence.compare(that.sequence)

      def reply() {
        publishMessage.reply(Nil)
      }
    }

    case class Transaction(publishMessage: InMessage) extends Publish {
      val sequence: Long = getParamLongValue(Sequence)(publishMessage)
      val timestamp: Timestamp = getParamLongValue(ReplicationParam.Timestamp)(publishMessage)
      val message: Message = publishMessage.getData[Message]
      val token = message.token

      message.timestamp = Some(timestamp)
    }

    case class KeepAlive(publishMessage: InMessage) extends Publish {
      val sequence: Long = getParamLongValue(Sequence)(publishMessage)
    }

    object CheckIdle

    object Commit

    object Kill

  }

  /**
   * Current timestamp gauge wrapper class per service member. Gauge with the same name and scope are not registered
   * more than once. If the gauge was directly referenced by the SubscriptionActor, the value would not be updated
   * by the future subscriptions of the same member. This would also pin SubscriptionActor in the heap forever since
   * the gauge keep reference to the code block providing the value. <p></p> The mamanger keeps one gauge wrapper per
   * member forever.
   */
  class SubscriptionCurrentTimestampGauge(member: ResolvedServiceMember) {
    var timestamp: Timestamp = Timestamp(0)

    private val gauge = metrics.gauge("current-timestamp", member.scopeName) {
      timestamp.value
    }
  }

  class SubscriptionActor(val subId: String, startTimestamp: Timestamp, endTimestamp: Option[Timestamp],
                          val subscribe: SubscriptionManagerProtocol.Subscribe,
                          currentTimestampGauge: SubscriptionCurrentTimestampGauge,
                          idGenerator: IdGenerator[Long]) extends Actor {

    private lazy val txReceivedMeter = metrics.meter("tx-received", "tx-received", member.scopeName)
    private lazy val txMissingMeter = metrics.meter("tx-missing", "tx-missing", member.scopeName)
    private lazy val idletimeoutMeter = metrics.meter("idle-timeout", "idle-timeout", member.scopeName)
    private lazy val errorMeter = metrics.meter("error", "error", member.scopeName)
    private lazy val keepAliveMeter = metrics.meter("keep-alive", "keep-alive", member.scopeName)
    private lazy val txWriteTimer = metrics.timer("tx-write", member.scopeName)

    import SubscriptionProtocol._

    val commitScheduler = new Scheduler(this, Commit, if (commitFrequency > 0) Random.nextInt(commitFrequency) else 0,
      commitFrequency, blockingMessage = true, autoStart = false)
    private val checkIdleScheduler = new Scheduler(this, CheckIdle, 1000, 1000, blockingMessage = true,
      autoStart = false)
    private var pendingTransactions: TreeSet[Publish] = TreeSet()
    private var consistentTimestamp: Option[Timestamp] = txLog.getLastLoggedRecord match {
      case Some(record) => record.consistentTimestamp
      case None => None
    }
    private var lastSequence = 0L
    private var lastReceiveTime = currentTime
    private var terminating = false

    private def txLog = subscribe.txLog

    def member = subscribe.member

    override def start() = {
      super.start()
      if (commitFrequency > 0) {
        commitScheduler.start()
      }
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
        case Some(publish) if publish.sequence == lastSequence + 1 => {
          pendingTransactions = pendingTransactions.tail

          publish match {
            case tx: Transaction => {
              // Add transaction message to transaction log and to consistent storage.
              trace("Storing (seq={}, subId={}) {}", tx.sequence, subId, tx.message)
              txWriteTimer.time {
                txLog.append {
                  Request(idGenerator.nextId, consistentTimestamp, tx.timestamp, tx.token, tx.message)
                }
                store.writeTransaction(tx.message)
                txLog.append {
                  Response(idGenerator.nextId, consistentTimestamp, tx.timestamp, tx.token, Success)
                }
              }

              // Update consistent timestamp
              consistentTimestamp = Some(tx.timestamp)
              currentTimestampGauge.timestamp = tx.timestamp
            }
            case _: KeepAlive => {
              keepAliveMeter.mark()
              trace("Keep alive (seq={}, subId={})", publish.sequence, subId)
            }
          }

          lastSequence = publish.sequence
          publish.reply()

          if (endTimestamp != None && consistentTimestamp == endTimestamp) {
            info("Processed last subscription transaction. Terminating subscription {} for {}.", subId, member)
            txLog.append {
              Index(idGenerator.nextId, consistentTimestamp)
            }
            manager ! SubscriptionManagerProtocol.TerminateSubscription(SubscriptionActor.this, None)
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
          case message: Publish if !terminating => {
            try {
              trace("Received publish (seq={}, subId={}) {}", message.sequence, subId, message)
              txReceivedMeter.mark()
              pendingTransactions += message
              lastReceiveTime = currentTime

              processHeadMessage()
            } catch {
              case e: Exception => {
                warn("Error processing publish message {}: ", message, e)
                errorMeter.mark()
                manager ! SubscriptionManagerProtocol.TerminateSubscription(SubscriptionActor.this, Some(e))
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
                  info("No pending transaction received for {} ms. Terminating subscription {} for {}.",
                    elapsedTime, subId, member)
                  new TimeoutException("No pending transaction received", Some(elapsedTime))
                } else {
                  txMissingMeter.mark()
                  info("Missing transactions (last written sequence {}, available sequences {}) after {} ms. " +
                    "Terminating subscription {} for {}.",
                    lastSequence, pendingTransactions.map(_.sequence), elapsedTime, subId, member)
                  new ConsistencyException()
                }

                manager ! SubscriptionManagerProtocol.TerminateSubscription(SubscriptionActor.this, Some(error))
                terminating = true
              }
            } catch {
              case e: Exception => {
                warn("Error checking for idle subscription {} for {}: ", subId, member, e)
                errorMeter.mark()
                manager ! SubscriptionManagerProtocol.TerminateSubscription(SubscriptionActor.this, Some(e))
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
                warn("Error commiting subscription {} transaction log for {}: ", subId, member, e)
                manager ! SubscriptionManagerProtocol.TerminateSubscription(SubscriptionActor.this, Some(e))
                terminating = true
              }
            } finally {
              reply(true)
            }
          }
          case Kill => {
            try {
              debug("Killing subscription {} actor {}: ", subId, member)
              checkIdleScheduler.cancel()
              commitScheduler.cancel()
              exit()
            } catch {
              case e: Exception => {
                warn("Error killing subscription {} actor {}: ", subId, member, e)
              }
            } finally {
              reply(true)
            }
          }
          case _ if terminating => {
            debug("Ignore actor message since subscription {} is terminating. {}", subId, member)
          }
        }
      }
    }
  }

}