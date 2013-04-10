package com.wajam.nrv.consistency.replication

import com.wajam.nrv.service._
import com.wajam.nrv.utils.timestamp.Timestamp
import com.wajam.nrv.data._
import com.wajam.nrv.consistency.{Consistency, ConsistentStore, ResolvedServiceMember}
import com.wajam.nrv.consistency.persistence.TransactionLog
import com.wajam.nrv.cluster.Node
import com.wajam.nrv.utils.{Scheduler, UuidStringGenerator}
import scala.actors.Actor
import collection.immutable.TreeSet
import ReplicationParam._
import com.wajam.nrv.Logging

class ReplicationPublisher(service: Service, store: ConsistentStore,
                           getTransactionLog: (ResolvedServiceMember) => TransactionLog,
                           getMemberCurrentConsistentTimestamp: (ResolvedServiceMember) => Option[Timestamp],
                           publishAction: Action, publishTPS: Int = 50, publishWindowSize: Int = 20)
  extends UuidStringGenerator with Logging {

  private val manager = new SubscriptionManagerActor

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

    case class Error(subscription: SubscriptionActor, exception: Option[Exception] = None)

    object Kill

  }

  class SubscriptionManagerActor extends Actor {

    import SubscriptionManagerProtocol._

    private case class PendingSubscription(subscribe: Subscribe)

    private var subscriptions: List[SubscriptionActor] = List()

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

    private def createSourceIterator(member: ResolvedServiceMember, from: Timestamp): ReplicationSourceIterator = {
      val txLog = getTransactionLog(member)
      txLog.firstRecord(None) match {
        case Some(logRecord) if (logRecord.timestamp <= from) => {
          // The first transaction log record is before the starting timestamp, use the transaction log as
          // replication source
          info("Using TransactionLogReplicationIterator. start={}, end={}, member={}", from,
            getMemberCurrentConsistentTimestamp(member), member)
          new TransactionLogReplicationIterator(member, from, txLog, getMemberCurrentConsistentTimestamp(member))
        }
        case Some(logRecord) => {
          // The first transaction log record is after the replication starting timestamp.
          // Use the consistent store as the replication source up to the first transaction log record.
          val to = logRecord.timestamp
          info("Using ConsistentStoreReplicationIterator. start={}, end={}, member={}", from, to, member)
          new ConsistentStoreReplicationIterator(member, from, to, store)
        }
        case None => {
          // There are no transaction log!!! Cannot replicate if transaction log is not enabled
          throw new Exception("No transaction log! Cannot replicate without transaction log")
        }
      }
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

    def act() {
      loop {
        react {
          case Subscribe(message) => {
            try {
              // TODO: limit the number of concurent replication subscription???
              implicit val request = message
              val start = getOptionalParamLongValue(Start).getOrElse(Long.MinValue)
              val token = getParamLongValue(Token)
              val member = createResolvedServiceMember(token)
              val source = createSourceIterator(member, start)

              val subscription = new SubscriptionActor(nextId, member, source, message.source)
              subscriptions = subscription :: subscriptions

              // Reply with a subscription response
              val response = new OutMessage(Seq((SubscriptionId -> subscription.subId), (Start -> start)))
              source.to.foreach(ts => response.parameters += (End -> ts.value))
              message.reply(response)

              subscription.start()
            } catch {
              case e: Exception => {
                // TODO: metric
                warn("Error processing subscribe request {}: ", message, e)
              }
            }
          }
          case Unsubscribe(message) => {
            try {
              implicit val request = message
              val id = getParamStringValue(SubscriptionId)

              subscriptions.find(_.subId == id).foreach(subscription => {
                subscription !? SubscriptionProtocol.Kill
                subscriptions = subscriptions.filterNot(_ == subscription)
              })
              message.reply(Seq())
            } catch {
              case e: Exception => {
                // TODO: metric
                warn("Error processing unsubscribe request {}: ", message, e)
              }
            }
          }
          case Error(subscriptionActor, exception) => {
            try {
              info("Got an error from the subscription actor. Stopping it. {}", subscriptionActor.member)
              subscriptionActor !? SubscriptionProtocol.Kill
              subscriptions = subscriptions.filterNot(_ == subscriptionActor)
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
            } catch {
              case e: Exception => {
                // TODO
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

  class SubscriptionActor(val subId: String, val member: ResolvedServiceMember,
                                  source: ReplicationSourceIterator, subscriber: Node) extends Actor with Logging {

    import SubscriptionProtocol._

    private val publishScheduler = new Scheduler(this, PublishNext, 1000, 1000 / publishTPS,
      blockingMessage = true, autoStart = false)
    private var pendingSequences: TreeSet[Long] = TreeSet()
    private var lastSequence = 0L
    private var error = false

    private def nextSequence = {
      lastSequence += 1
      lastSequence
    }

    override def start() = {
      super.start()
      if (publishTPS > 0) {
        publishScheduler.start()
      }
      this
    }

    private def canPublishMore: Boolean = {
      pendingSequences.headOption match {
        case Some(firstPendingSequence) => lastSequence - firstPendingSequence < publishWindowSize
        case None => true
      }
    }

    private def onPublishReply(sequence: Long)(response: Message, optException: Option[Exception]) {
      optException match {
        case Some(e) => {
          // TODO: Log exception + end subscription
          info("Received an error response from the subscriber (seq={}): ", e)
          manager ! SubscriptionManagerProtocol.Error(SubscriptionActor.this, Some(e))
          error = true
        }
        case None => {
          info("Received an publish response from the subscriber (seq={}).", sequence)
          this ! Ack(sequence)
        }
      }
    }

    def act() {
      loop {
        react {
          case PublishNext if !error => {
            try {
              if (source.hasNext && canPublishMore) {
                source.next() match {
                  case Some(txMessage) => {
                    val sequence = nextSequence
                    // Must fail if timestamp is missing
                    val timestamp = Consistency.getMessageTimestamp(txMessage).get
                    val params: Seq[(String, MValue with Product with Serializable)] =
                      Seq((Sequence -> sequence), (SubscriptionId -> subId),
                        (ReplicationParam.Timestamp -> timestamp.value))
                    val publishMessage = new OutMessage(params = params, data = txMessage,
                      onReply = onPublishReply(sequence), responseTimeout = service.responseTimeout)
                    publishMessage.destination = new Endpoints(Seq(new Shard(-1, Seq(new Replica(-1, subscriber)))))

                    info("Publishing message to subscriber (seq={}).", sequence, txMessage)

                    publishAction.call(publishMessage)
                    pendingSequences += sequence
                  }
                  case None => // No more message available at this time
                }
              }

            } catch {
              case e: Exception => {
                info("Error publishing a transaction (subid={}). {}: ", subId, member, e)
                manager ! SubscriptionManagerProtocol.Error(SubscriptionActor.this, Some(e))
                error = true
              }
            } finally {
              reply(true)
            }
          }
          case Ack(sequence) if !error => {
            try {
              pendingSequences -= sequence
            } catch {
              case e: Exception => {
                info("Error acknoledging transaction (subid={}, seq={}). {}: ", subId, sequence, member, e)
                manager ! SubscriptionManagerProtocol.Error(SubscriptionActor.this, Some(e))
                error = true
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
                info("Error killing actor (subid={}). {}: ", subId, member ,e)
              }
            } finally {
              reply(true)
            }
          }
          case _ if error => {
            // TODO: debug
            info("Ignore actor message since this subscription is already terminated. {}", member)
          }
        }
      }
    }
  }

}
