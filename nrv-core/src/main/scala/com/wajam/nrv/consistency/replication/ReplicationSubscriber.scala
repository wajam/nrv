package com.wajam.nrv.consistency.replication

import com.wajam.nrv.service.{Action, Service}
import com.wajam.nrv.consistency.{ResolvedServiceMember, ConsistentStore}
import com.wajam.nrv.data.{MValue, InMessage, Message}
import com.wajam.nrv.utils.timestamp.Timestamp
import actors.Actor
import com.wajam.nrv.utils.{CurrentTime, TimestampIdGenerator, IdGenerator, Scheduler}
import collection.immutable.TreeSet
import com.wajam.nrv.consistency.persistence.TransactionLog
import ReplicationParam._
import com.wajam.nrv.consistency.persistence.LogRecord.{Response, Request}
import com.wajam.nrv.consistency.persistence.LogRecord.Response.Success
import annotation.tailrec
import com.wajam.nrv.Logging
import java.util.{TimerTask, Timer}

class ReplicationSubscriber(service: Service, store: ConsistentStore, maxIdleDurationInMs: Long)
  extends CurrentTime with Logging {

  private val manager = new SubscriptionManagerActor

  def start() {
    manager.start()
  }

  def stop() {
    manager !? SubscriptionManagerProtocol.Kill
  }

  def subscribe(member: ResolvedServiceMember, txLog: TransactionLog, delay: Long, subscribeAction: Action,
                unsubscribeAction: Action, onSubscriptionEnd: => Unit) {
    manager ! SubscriptionManagerProtocol.Subscribe(member, txLog, subscribeAction, unsubscribeAction,
      () => onSubscriptionEnd, delay)
  }

  def unsubscribe(member: ResolvedServiceMember) {
    manager ! SubscriptionManagerProtocol.Unsubscribe(member)
  }

  def handlePublishMessage(message: InMessage) {
    manager ! SubscriptionManagerProtocol.Publish(message)
  }

  object SubscriptionManagerProtocol {

    case class Subscribe(member: ResolvedServiceMember, txLog: TransactionLog, subscribeAction: Action,
                         unsubscribeAction: Action, onSubscriptionEnd: () => Unit, delay: Long = 0)

    case class SubscribeResponse(subscribe: Subscribe, response: Message, optException: Option[Exception])

    case class Unsubscribe(member: ResolvedServiceMember)

    case class Publish(message: InMessage)

    case class Error(subscription: SubscriptionActor, exception: Option[Exception] = None)

    object Kill

  }

  class SubscriptionManagerActor extends Actor {

    import SubscriptionManagerProtocol._

    private case class PendingSubscription(subscribe: Subscribe)

    private val timer = new Timer("SubscriptionManagerActor-Timer")
    private var subscriptions: Map[ResolvedServiceMember, Either[PendingSubscription, SubscriptionActor]] = Map()

    def act() {
      loop {
        react {
          case subscribe: Subscribe => {
            try {
              subscriptions.get(subscribe.member) match {
                case Some(Left(_)) => {
                  info("Ignore new subscribe request. Already have a pending subscription registered for {}.",
                    subscribe.member)
                }
                case Some(Right(_)) => {
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
                // TODO: metric
                warn("Error processing subscribe for {}", subscribe.member, e)
                subscriptions -= (subscribe.member)
                subscribe.onSubscriptionEnd()
              }
            }
          }
          case PendingSubscription(subscribe) => {
            try {
              subscriptions.get(subscribe.member) match {
                case Some(Left(_)) => {
                  info("Send subscribe request for pending subscription. {}", subscribe.member)
                  var params: Map[String, MValue] = Map(ReplicationParam.Token -> subscribe.member.token.toString)
                  subscribe.txLog.getLastLoggedRecord.map(_.consistentTimestamp) match {
                    case Some(Some(lastTimestamp)) => {
                      val startTimestamp = lastTimestamp.value + 1
                      params += (ReplicationParam.Start -> startTimestamp.toString)
                    }
                    case _ => {
                      // No records in transaction log. Omit start if the local store is empty.
                      // The replication publisher will send all the transactions from the beginning.
                      // TODO: verify store is empty
                    }
                  }
                  subscribe.subscribeAction.call(params,
                    onReply = SubscriptionManagerActor.this ! SubscribeResponse(subscribe, _, _))
                }
                case Some(Right(_)) => {
                  warn("Do not process pending subscription. Already have an active subscription for {}.",
                    subscribe.member)
                }
                case None => {
                  info("Do not process pending subscription. No more pending subscription registered for {}.",
                    subscribe.member)
                }
              }
            } catch {
              case e: Exception => {
                // TODO: metric
                warn("Error processing delayed subscribe for {}", subscribe.member, e)
                subscriptions -= (subscribe.member)
                subscribe.onSubscriptionEnd()
              }
            }
          }
          case SubscribeResponse(subscribe, message, exception) => {
            try {
              implicit val response = message

              exception match {
                case Some(e) => {
                  // TODO: metric
                  warn("Got a subscribe response error for {}: ", subscribe.member, e)
                  subscriptions -= (subscribe.member)
                  subscribe.onSubscriptionEnd()
                }
                case None => {
                  // TODO: error handling + more usefull description
                  subscriptions.get(subscribe.member) match {
                    case Some(Left(_)) => {
                      info("Activate subscription from response {}.", subscribe.member, message)
                      val subscriptionId = getParamStringValue(SubscriptionId)
                      val startTimestamp = getParamLongValue(Start)
                      val endTimestamp = getOptionalParamLongValue(End).map(ts => Timestamp(ts))

                      val subscription = new SubscriptionActor(subscriptionId, startTimestamp, endTimestamp, subscribe,
                        new TimestampIdGenerator)
                      subscriptions += (subscribe.member -> Right(subscription))
                      subscription.start()
                    }
                    case Some(Right(_)) => {
                      warn("Do not activate subscription. Already have an active subscription for {}.",
                        subscribe.member)
                      // TODO: unsubscribe from replication source?
                    }
                    case None => {
                      info("Do not activate subscription. No more subscription registered for {}.",
                        subscribe.member)
                      // TODO: unsubscribe from replication source?
                    }
                  }
                }
              }
            } catch {
              case e: Exception => {
                // TODO: metric
                warn("Error processing subscribe response for {}", subscribe.member, e)
                subscriptions -= (subscribe.member)
                subscribe.onSubscriptionEnd()
              }
            }
          }
          case Unsubscribe(member) => {
            try {
              subscriptions.get(member) match {
                case Some(Left(PendingSubscription(subscribe))) => {
                  info("Unsubscribe. Remove pending subscription. {}", member)
                  subscriptions -= (member)
                  subscribe.onSubscriptionEnd()
                }
                case Some(Right(subscription)) => {
                  info("Unsubscribe. Remove active subscription for. {}", member)
                  // TODO: unsubscribe from replication source?
                  subscriptions -= (member)
                  subscription ! SubscriptionProtocol.Kill
                }
                case None => {
                  info("Unsubscribe. No action taken, no subscription registered for {}.", member)
                }
              }
            } catch {
              case e: Exception => {
                // TODO: metric
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
                  subscription ! SubscriptionProtocol.PendingTransaction(message)
                }
                case None => // TODO: unsubscribe from replication source?
              }
            } catch {
              case e: Exception => {
                // TODO: metric
                warn("Error processing publish {}", message, e)
              }
            }
          }
          case Error(subscriptionActor, exception) => {
            try {
              info("Got an error from the subscription actor. Unsubscribing from {}.", subscriptionActor.member)
              subscriptions.get(subscriptionActor.member) match {
                case Some(Right(subscription)) if subscription.subId == subscriptionActor.subId => {
                  subscriptions -= (subscriptionActor.member)
                  subscription.subscribe.onSubscriptionEnd()
                  subscriptionActor !? SubscriptionProtocol.Kill
                }
                case _ => // Not subscribed anymore to that subscription. Take no local action.
              }
              // TODO: unsubscribe from replication source?
            } catch {
              case e: Exception => {
                // TODO: metric
                warn("Error processing subscription error. {}", subscriptionActor.member, e)
              }
            }
          }
          case Kill => {
            try {
              subscriptions.valuesIterator.map({
                case Right(subscription) => subscription
              }).foreach(_ !? SubscriptionProtocol.Kill)
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

    case class PendingTransaction(private val publishMessage: InMessage)
      extends Ordered[PendingTransaction] {
      val sequence: Long = getParamLongValue(Sequence)(publishMessage)
      val timestamp: Timestamp = getParamLongValue(ReplicationParam.Timestamp)(publishMessage)
      val message: Message = publishMessage.messageData.asInstanceOf[Message]
      val token = message.token

      def compare(that: PendingTransaction) = sequence.compare(that.sequence)

      def reply() {
        publishMessage.reply(Seq())
      }
    }

    object CheckIdle

    object Kill

  }

  class SubscriptionActor(val subId: String, startTimestamp: Timestamp, to: Option[Timestamp],
                          val subscribe: SubscriptionManagerProtocol.Subscribe,
                          idGenerator: IdGenerator[Long]) extends Actor {

    import SubscriptionProtocol._

    private val checkIdleScheduler = new Scheduler(this, CheckIdle, 1000, 1000, blockingMessage = true,
      autoStart = false)
    private var pendingTransactions: TreeSet[PendingTransaction] = TreeSet()
    private var consistentTimestamp: Option[Timestamp] = txLog.getLastLoggedRecord match {
      case Some(record) => record.consistentTimestamp
      case None => None
    }
    private var lastSequence = 0L
    private var lastAddedTime = currentTime
    private var error = false

    private def txLog = subscribe.txLog

    def member = subscribe.member

    override def start() = {
      super.start()
      checkIdleScheduler.start()
      this
    }

    /**
     * Add the head pending transaction to the transaction log and the consistent store if its sequence number follows
     * directly the last added transaction sequence number. No gap in sequence is allowed to prevent skipping
     * transactions.
     */
    @tailrec
    private def processHeadTransaction() {
      pendingTransactions.headOption match {
        case Some(tx) if tx.sequence == lastSequence + 1 => {
          pendingTransactions = pendingTransactions.tail

          // Add transaction to transaction log and to consistent storage.
          txLog.append {
            Request(idGenerator.nextId, consistentTimestamp, tx.timestamp, tx.token, tx.message)
          }
          store.writeTransaction(tx.message)
          txLog.append {
            Response(idGenerator.nextId, consistentTimestamp, tx.timestamp, tx.token, Success)
          }

          // Update consistent timestamp and last added sequence number
          consistentTimestamp = Some(tx.timestamp)
          lastSequence = tx.sequence

          tx.reply()

          // Process the new head recusrsively
          processHeadTransaction()
        }
        case _ => // Do nothing as the head transaction is empty or not the next sequence number.
      }
    }

    def act() {
      loop {
        react {
          case tx: PendingTransaction if !error => {
            try {
              pendingTransactions += tx
              lastAddedTime = currentTime

              processHeadTransaction()
            } catch {
              case e: Exception => {
                warn("Error processing new pending transaction {}: ", tx, e)
                manager ! SubscriptionManagerProtocol.Error(SubscriptionActor.this, Some(e))
                error = true
              }
            }
          }
          case CheckIdle if !error => {
            try {
              val elapsedTime = currentTime - lastAddedTime
              if (elapsedTime > maxIdleDurationInMs) {
                warn("No pending transaction received for {} ms. Terminating subscription for.", elapsedTime, member)
                manager ! SubscriptionManagerProtocol.Error(SubscriptionActor.this, None)
                error = true
              }
            } catch {
              case e: Exception => {
                warn("Error checking for idle subscription {}: ", member, e)
                manager ! SubscriptionManagerProtocol.Error(SubscriptionActor.this, Some(e))
                error = true
              }
            } finally {
              reply(true)
            }
          }
          case Kill => {
            try {
              checkIdleScheduler.cancel()
              exit()
            } catch {
              case e: Exception => {
                warn("Error killing subscription actor {}: ", member, e)
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