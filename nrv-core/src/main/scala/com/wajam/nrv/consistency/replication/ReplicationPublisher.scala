package com.wajam.nrv.consistency.replication

import com.wajam.nrv.service._
import com.wajam.nrv.utils.timestamp.Timestamp
import com.wajam.nrv.data._
import com.wajam.nrv.consistency.{ConsistentStore, ResolvedServiceMember}
import com.wajam.nrv.consistency.persistence.TransactionLog
import com.wajam.nrv.cluster.Node
import com.wajam.nrv.utils.{CurrentTime, Scheduler, UuidStringGenerator}
import scala.actors.Actor
import collection.immutable.TreeSet
import ReplicationParam._
import com.wajam.nrv.Logging
import com.yammer.metrics.scala.Instrumented

/**
 * Manage all replication subscriptions the local service node is acting as a replication source. The replication
 * source must be the master replica and the replication source must have transaction logs enabled.
 */
class ReplicationPublisher(service: Service, store: ConsistentStore,
                           getTransactionLog: (ResolvedServiceMember) => TransactionLog,
                           getMemberCurrentConsistentTimestamp: (ResolvedServiceMember) => Option[Timestamp],
                           publishAction: Action, publishTps: Int, publishWindowSize: Int, maxIdleDurationInMs: Long)
  extends CurrentTime with UuidStringGenerator with Logging with Instrumented {

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

  def terminateSubscriptions(member: ResolvedServiceMember) {
    manager ! SubscriptionManagerProtocol.TerminateSubscriptions(member)
  }

  def handleSubscribeMessage(message: InMessage) {
    manager ! SubscriptionManagerProtocol.Subscribe(message)
  }

  def handleUnsubscribeMessage(implicit message: InMessage) {
    manager ! SubscriptionManagerProtocol.Unsubscribe(message)
  }

  object SubscriptionManagerProtocol {

    case class Subscribe(message: InMessage)

    case class Unsubscribe(message: InMessage)

    case class TerminateSubscriptions(member: ResolvedServiceMember)

    case class TerminateSubscription(subscription: SubscriptionActor, error: Option[Exception] = None)

    object Kill

  }

  class SubscriptionManagerActor extends Actor {

    private lazy val subscribeMeter = metrics.meter("subscribe", "subscribe", serviceScope)
    private lazy val subscribeErrorMeter = metrics.meter("subscribe-error", "subscribe-error", serviceScope)

    private lazy val unsubscribeMeter = metrics.meter("unsubscribe", "unsubscribe", serviceScope)
    private lazy val unsubscribeErrorMeter = metrics.meter("unsubscribe-error", "subscribe-error", serviceScope)

    private lazy val terminateErrorMeter = metrics.meter("terminate-error", "terminate-error", serviceScope)

    import SubscriptionManagerProtocol._

    private var subscriptions: List[SubscriptionActor] = Nil

    def subscriptionsCount = subscriptions.size

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

    private def createSourceIterator(member: ResolvedServiceMember, startTimestamp: Timestamp,
                                     isLiveReplication: Boolean): ReplicationSourceIterator = {
      val txLog = getTransactionLog(member)
      val sourceIterator = txLog.firstRecord(None) match {
        case Some(logRecord) if isLiveReplication && logRecord.timestamp <= startTimestamp => {
          // Live replication mode. Use the transaction log if the first transaction log record is before the starting
          // timestamp.
          info("Using TransactionLogReplicationIterator. start={}, end={}, member={}", startTimestamp,
            getMemberCurrentConsistentTimestamp(member), member)
          new TransactionLogReplicationIterator(member, startTimestamp, txLog, getMemberCurrentConsistentTimestamp(member))
        }
        case Some(_) => {
          // Not in live replication mode or the first transaction log record is after the replication starting
          // timestamp or store source is forced. Use the consistent store as the replication source up to the
          // current member consistent timestamp.
          val endTimestamp = getMemberCurrentConsistentTimestamp(member).get
          info("Using ConsistentStoreReplicationIterator. start={}, end={}, member={}",
            startTimestamp, endTimestamp, member)
          new ConsistentStoreReplicationIterator(member, startTimestamp, endTimestamp, store)
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
          case Subscribe(message) => {
            try {
              // TODO: limit the number of concurent replication subscription? Global limit or per service member?
              subscribeMeter.mark()
              debug("Received a subscribe request {}", message)

              implicit val request = message
              val start = getOptionalParamLongValue(Start).getOrElse(Long.MinValue)
              val token = getParamLongValue(Token)
              val isLiveReplication = getOptionalParamStringValue(Mode).map(ReplicationMode(_)) match {
                case Some(ReplicationMode.Live) => true
                case _ => false
              }
              val member = createResolvedServiceMember(token)
              val source = createSourceIterator(member, start, isLiveReplication)

              val subscription = new SubscriptionActor(nextId, member, source, message.source)
              subscriptions = subscription :: subscriptions

              // Reply with a subscription response
              val response = new OutMessage(Seq((SubscriptionId -> subscription.subId), (Start -> start)))
              source.end.foreach(ts => response.parameters += (End -> ts.value))
              message.reply(response)

              subscription.start()
            } catch {
              case e: Exception => {
                subscribeErrorMeter.mark()
                warn("Error processing subscribe request {}: ", message, e)
              }
            }
          }
          case Unsubscribe(message) => {
            try {
              unsubscribeMeter.mark()
              debug("Received an unsubscribe request {}", message)

              implicit val request = message
              val id = getParamStringValue(SubscriptionId)
              subscriptions.find(_.subId == id).foreach(subscription => {
                subscription !? SubscriptionProtocol.Kill
                subscriptions = subscriptions.filterNot(_ == subscription)
              })
              message.reply(Nil)
            } catch {
              case e: Exception => {
                unsubscribeErrorMeter.mark()
                warn("Error processing unsubscribe request {}: ", message, e)
              }
            }
          }
          case TerminateSubscription(subscriptionActor, exception) => {
            try {
              subscriptions.find(_ == subscriptionActor).foreach(subscription => {
                info("Subscription actor {} wants to be terminated. Dutifully perform euthanasia! {}",
                  subscriptionActor.subId, subscriptionActor.member)
                subscription !? SubscriptionProtocol.Kill
                subscriptions = subscriptions.filterNot(_ == subscription)
              })
            } catch {
              case e: Exception => {
                terminateErrorMeter.mark()
                warn("Error processing terminate subscription {}: ", subscriptionActor.member, e)
              }
            }
          }
          case TerminateSubscriptions(member) => {
            try {
              subscriptions.find(_.member == member).foreach(subscription => {
                info("Subscription {} is terminated. {}",
                  subscription.subId, member)
                subscription !? SubscriptionProtocol.Kill
                subscriptions = subscriptions.filterNot(_ == subscription)
              })
            } catch {
              case e: Exception => {
                terminateErrorMeter.mark()
                warn("Error processing terminate all subscriptions {}: ", e)
              }
            }
          }
          case Kill => {
            try {
              subscriptions.foreach(_ !? SubscriptionProtocol.Kill)
              subscriptions = Nil
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

    object PublishNext

    case class Ack(sequence: Long)

    object Kill

  }

  class SubscriptionActor(val subId: String, val member: ResolvedServiceMember, source: ReplicationSourceIterator,
                          subscriber: Node) extends Actor with Logging {

    private lazy val publishMeter = metrics.meter("publish", "publish", member.scopeName)
    private lazy val publishErrorMeter = metrics.meter("publish-error", "publish-error", member.scopeName)

    private lazy val ackMeter = metrics.meter("ack", "ack", member.scopeName)
    private lazy val ackTimeoutMeter = metrics.meter("ack-timeout", "ack-timeout", member.scopeName)
    private lazy val ackErrorMeter = metrics.meter("ack-error", "ack-error", member.scopeName)

    import SubscriptionProtocol._

    private val publishScheduler = new Scheduler(this, PublishNext, 1000, 1000 / publishTps,
      blockingMessage = true, autoStart = false)
    private var pendingSequences: TreeSet[Long] = TreeSet()
    private var lastSendTime = currentTime
    private var lastSequence = 0L
    private var terminating = false

    private def nextSequence = {
      lastSequence += 1
      lastSequence
    }

    override def start() = {
      super.start()
      if (publishTps > 0) {
        publishScheduler.start()
      }
      this
    }

    private def currentWindowSize: Int = {
      pendingSequences.headOption match {
        case Some(firstPendingSequence) => (lastSequence - firstPendingSequence).toInt
        case None => 0
      }
    }

    private def sendKeepAlive() {
      sendPublish(None)
    }

    private def sendPublish(transaction: Option[Message]) {

      def onReply(sequence: Long)(response: Message, optException: Option[Exception]) {
        optException match {
          case Some(e) => {
            debug("Received an error response from the subscriber (seq={}): ", sequence, e)
            manager ! SubscriptionManagerProtocol.TerminateSubscription(SubscriptionActor.this, Some(e))
            terminating = true
          }
          case None => {
            trace("Received an publish response from the subscriber (seq={}).", sequence)
            this ! Ack(sequence)
          }
        }
      }

      val sequence = nextSequence
      var params: Map[String, MValue] = Map()
      val data = transaction match {
        case Some(message) => {
          val timestamp = message.timestamp.get // Must fail if timestamp is missing
          params += (ReplicationParam.Timestamp -> timestamp.value)
          message
        }
        case None => null // Keep-alive
      }
      params += (Sequence -> sequence)
      params += (SubscriptionId -> subId)

      val publishMessage = new OutMessage(params = params, data = data,
        onReply = onReply(sequence), responseTimeout = service.responseTimeout)
      publishMessage.destination = new Endpoints(Seq(new Shard(-1, Seq(new Replica(-1, subscriber)))))
      publishAction.call(publishMessage)

      pendingSequences += sequence
      lastSendTime = currentTime
      publishMeter.mark()

      trace("Published message to subscriber (seq={}, window={}).", sequence, currentWindowSize)
    }

    def act() {
      loop {
        react {
          case PublishNext if !terminating => {
            try {
              if (currentWindowSize < publishWindowSize) {
                if (source.hasNext) {
                  source.next() match {
                    case Some(txMessage) => sendPublish(Some(txMessage))
                    case None => {
                      // No more message available at this time but the replication source is not empty
                      // (i.e. hasNext == true). We are expecting more transaction messages to be available soon.
                      // Meanwhile send a keep-alive message every few seconds to subscriber to prevent subscription
                      // idle timeout.
                      if (currentTime - lastSendTime > math.min(maxIdleDurationInMs / 4, 5000)) {
                        sendKeepAlive()
                      }
                    }
                  }
                } else {
                  info("Replication source is exhausted! Terminating subscription {} for {}.", subId, member)
                  manager ! SubscriptionManagerProtocol.TerminateSubscription(SubscriptionActor.this, None)
                  terminating = true
                }
              } else {
                // Window size is full. Cancel subscription if havent received an ack for a while.
                val elapsedTime = currentTime - lastSendTime
                if (elapsedTime > maxIdleDurationInMs) {
                  ackTimeoutMeter.mark()
                  info("No ack received for {} ms. Terminating subscription {} for {}.", elapsedTime, subId, member)
                  manager ! SubscriptionManagerProtocol.TerminateSubscription(SubscriptionActor.this, None)
                  terminating = true
                }
              }
            } catch {
              case e: Exception => {
                publishErrorMeter.mark()
                info("Error publishing a transaction (subid={}). {}: ", subId, member, e)
                manager ! SubscriptionManagerProtocol.TerminateSubscription(SubscriptionActor.this, Some(e))
                terminating = true
              }
            } finally {
              reply(true)
            }
          }
          case Ack(sequence) if !terminating => {
            try {
              pendingSequences -= sequence
              ackMeter.mark()
            } catch {
              case e: Exception => {
                ackErrorMeter.mark()
                info("Error acknoledging transaction (subid={}, seq={}). {}: ", subId, sequence, member, e)
                manager ! SubscriptionManagerProtocol.TerminateSubscription(SubscriptionActor.this, Some(e))
                terminating = true
              }
            }
          }
          case Kill => {
            try {
              try {
                publishScheduler.cancel()
                source.close()
              } finally {
                exit()
              }
            } catch {
              case e: Exception => {
                info("Error killing actor (subid={}). {}: ", subId, member, e)
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
