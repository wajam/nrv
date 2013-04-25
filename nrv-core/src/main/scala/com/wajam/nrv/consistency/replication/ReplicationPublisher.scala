package com.wajam.nrv.consistency.replication

import com.wajam.nrv.service._
import com.wajam.nrv.utils.timestamp.Timestamp
import com.wajam.nrv.data._
import com.wajam.nrv.consistency.{Consistency, ConsistentStore, ResolvedServiceMember}
import com.wajam.nrv.consistency.persistence.TransactionLog
import com.wajam.nrv.cluster.Node
import com.wajam.nrv.utils.{CurrentTime, Scheduler, UuidStringGenerator}
import scala.actors.Actor
import collection.immutable.TreeSet
import ReplicationParam._
import com.wajam.nrv.Logging
import com.yammer.metrics.scala.Instrumented

/**
 * Manage all replication subscriptions the local service is acting as a replication source. When strictSource is
 * enabled (recommended default), the replication source must be the master replica and the replication source must
 * have enabled transaction logs.
 */
class ReplicationPublisher(service: Service, store: ConsistentStore,
                           getTransactionLog: (ResolvedServiceMember) => TransactionLog,
                           getMemberCurrentConsistentTimestamp: (ResolvedServiceMember) => Option[Timestamp],
                           publishAction: Action, publishTps: Int, publishWindowSize: Int, maxIdleDurationInMs: Long,
                           strictSource: Boolean = true)
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

  def handleSubscribeMessage(message: InMessage) {
    manager ! SubscriptionManagerProtocol.Subscribe(message)
  }

  def handleUnsubscribeMessage(implicit message: InMessage) {
    manager ! SubscriptionManagerProtocol.Unsubscribe(message)
  }

  object SubscriptionManagerProtocol {

    case class Subscribe(message: InMessage)

    case class Unsubscribe(message: InMessage)

    case class Terminate(subscription: SubscriptionActor, error: Option[Exception] = None)

    object Kill

  }

  class SubscriptionManagerActor extends Actor {

    private lazy val subscribeMeter = metrics.meter("subscribe", "subscribe", serviceScope)
    private lazy val subscribeErrorMeter = metrics.meter("subscribe-error", "subscribe-error", serviceScope)

    private lazy val unsubscribeMeter = metrics.meter("unsubscribe", "unsubscribe", serviceScope)
    private lazy val unsubscribeErrorMeter = metrics.meter("unsubscribe-error", "subscribe-error", serviceScope)

    import SubscriptionManagerProtocol._

    private var subscriptions: List[SubscriptionActor] = Nil

    def subscriptionsCount = subscriptions.size

    private def createResolvedServiceMember(token: Long): ResolvedServiceMember = {
      service.getMemberAtToken(token) match {
        case Some(member) if service.cluster.isLocalNode(member.node) => {
          ResolvedServiceMember(service.name, token, service.getMemberTokenRanges(member))
        }
        case Some(member) => {
          if (strictSource) {
            throw new Exception("local node not master of token '%d' service member (master=%s)".format(token, member.node))
          } else {
            ResolvedServiceMember(service.name, token, service.getMemberTokenRanges(member))
          }
        }
        case None => {
          throw new Exception("token '%d' service member not found".format(token))
        }
      }
    }

    private def createSourceIterator(member: ResolvedServiceMember, startTimestamp: Timestamp): ReplicationSourceIterator = {
      val txLog = getTransactionLog(member)
      val sourceIterator = txLog.firstRecord(None) match {
        case Some(logRecord) if (logRecord.timestamp <= startTimestamp) => {
          // The first transaction log record is before the starting timestamp, use the transaction log as
          // replication source
          info("Using TransactionLogReplicationIterator. start={}, end={}, member={}", startTimestamp,
            getMemberCurrentConsistentTimestamp(member), member)
          new TransactionLogReplicationIterator(member, startTimestamp, txLog, getMemberCurrentConsistentTimestamp(member))
        }
        case Some(logRecord) => {
          // The first transaction log record is after the replication starting timestamp.
          // Use the consistent store as the replication source up to the first transaction log record.
          val endTimestamp = logRecord.timestamp
          info("Using ConsistentStoreReplicationIterator. start={}, end={}, member={}",
            startTimestamp, endTimestamp, member)
          new ConsistentStoreReplicationIterator(member, startTimestamp, endTimestamp, store)
        }
        case None => {
          // There are no transaction log!!! Cannot replicate if transaction log is not enabled
          if (strictSource) {
            throw new Exception("No transaction log! Cannot replicate without transaction log")
          } else {
            // Replicate from the store using the currently most recent store timestamp as upper boundary
            val endTimestamp = store.getLastTimestamp(member.ranges).getOrElse(Timestamp(Long.MinValue))
            info("Using ConsistentStoreReplicationIterator. start={}, end={}, member={}",
              startTimestamp, endTimestamp, member)
            new ConsistentStoreReplicationIterator(member, startTimestamp, endTimestamp, store)
          }
        }
      }
      // Exclude startTimestamp
      sourceIterator.withFilter{
        case Some(msg) => Consistency.getMessageTimestamp(msg).get > startTimestamp
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
              val member = createResolvedServiceMember(token)
              val source = createSourceIterator(member, start)

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
          case Terminate(subscriptionActor, exception) => {
            try {
              subscriptions.find(_ == subscriptionActor).foreach(subscription => {
                info("Subscription actor {} wants to be terminated. Dutifully perform euthanasia! {}",
                  subscriptionActor.subId, subscriptionActor.member)
                subscription !? SubscriptionProtocol.Kill
                subscriptions = subscriptions.filterNot(_ == subscription)
              })
            } catch {
              case e: Exception => {
                // TODO: metric
                warn("Error processing subscription error {}: ", subscriptionActor.member, e)
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

    private def onPublishReply(sequence: Long)(response: Message, optException: Option[Exception]) {
      optException match {
        case Some(e) => {
          debug("Received an error response from the subscriber (seq={}): ", e)
          manager ! SubscriptionManagerProtocol.Terminate(SubscriptionActor.this, Some(e))
          terminating = true
        }
        case None => {
          trace("Received an publish response from the subscriber (seq={}).", sequence)
          this ! Ack(sequence)
        }
      }
    }

    def act() {
      loop {
        react {
          case PublishNext if !terminating => {
            try {
              if (currentWindowSize < publishWindowSize) {
                if (source.hasNext) {
                  source.next() match {
                    case Some(txMessage) => {
                      val sequence = nextSequence
                      // Must fail if timestamp is missing
                      val timestamp = Consistency.getMessageTimestamp(txMessage).get
                      val params: Seq[(String, MValue)] =
                        Seq((Sequence -> sequence), (SubscriptionId -> subId),
                          (ReplicationParam.Timestamp -> timestamp.value))
                      val publishMessage = new OutMessage(params = params, data = txMessage,
                        onReply = onPublishReply(sequence), responseTimeout = service.responseTimeout)
                      publishMessage.destination = new Endpoints(Seq(new Shard(-1, Seq(new Replica(-1, subscriber)))))

                      trace("Publishing message to subscriber (seq={}, window={}).", sequence, currentWindowSize, txMessage)

                      publishAction.call(publishMessage)
                      pendingSequences += sequence
                      lastSendTime = currentTime
                      publishMeter.mark()
                    }
                    case None => {
                      // No more message available at this time
                      // TODO: Send idle message to subscriber from time to time to say "Hi, I am still here but I have nothing for you at this time!"
                    }
                  }
                } else {
                  info("Replication source is exhausted! Terminating subscription {} for {}.", subId, member)
                  manager ! SubscriptionManagerProtocol.Terminate(SubscriptionActor.this, None)
                  terminating = true
                }
              } else {
                // Window size is full. Cancel subscription if havent received an ack for a while.
                val elapsedTime = currentTime - lastSendTime
                if (elapsedTime > maxIdleDurationInMs) {
                  ackTimeoutMeter.mark()
                  info("No ack received for {} ms. Terminating subscription {} for {}.", elapsedTime, subId, member)
                  manager ! SubscriptionManagerProtocol.Terminate(SubscriptionActor.this, None)
                  terminating = true
                }
              }
            } catch {
              case e: Exception => {
                publishErrorMeter.mark()
                info("Error publishing a transaction (subid={}). {}: ", subId, member, e)
                manager ! SubscriptionManagerProtocol.Terminate(SubscriptionActor.this, Some(e))
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
                manager ! SubscriptionManagerProtocol.Terminate(SubscriptionActor.this, Some(e))
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
