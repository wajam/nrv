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

  private var subscriptions: List[SubscriptionActor] = List()

  def handleSubscribeMessage(implicit message: InMessage) {
    // TODO: catch error and reply

    val start = getOptionalParamLongValue(Start).getOrElse(Long.MinValue)
    val token = getParamLongValue(Token)
    val member = createResolvedServiceMember(token)
    val source = createSourceIterator(member, start)

    // TODO: synchronize addition and maybe more
    this.synchronized {
      val sub = new SubscriptionActor(nextId, member, source, message.source)
      subscriptions = sub :: subscriptions

      // TODO: reply
      val reply = new OutMessage(Seq((SubscriptionId -> sub.subscriptionId), (Start -> start)))
      source.to.foreach(ts => reply.parameters += (End -> ts.value))
      message.reply(reply)

      sub.start()
    }
  }

  def handleUnsubscribeMessage(implicit message: InMessage) {
    val id = getParamStringValue(SubscriptionId)

    // TODO: kill actor and close erything
    this.synchronized {
      subscriptions.find(_.subscriptionId == id).foreach(sub => {
        sub !? SubscriptionProtocol.Kill
        subscriptions = subscriptions.filterNot(_ == sub)
      })
    }
  }

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

  private[replication] def publishNext(subscriptionId: String) {
    subscriptions.find(_.subscriptionId == subscriptionId).foreach(_ !? SubscriptionProtocol.PublishNext)
  }

  object SubscriptionProtocol {

    object PublishNext

    object Kill

    case class Ack(sequence: Long)

  }

  private class SubscriptionActor(val subscriptionId: String, member: ResolvedServiceMember,
                                  source: ReplicationSourceIterator, subscriber: Node) extends Actor with Logging {
    import SubscriptionProtocol._

    private val publishScheduler = new Scheduler(this, PublishNext, 1000, 1000 / publishTPS,
      blockingMessage = true, autoStart = false)
    private var pendingSequences: TreeSet[Long] = TreeSet()
    private var lastSequence = 0L

    private def nextSequence = {
      lastSequence += 1
      lastSequence
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
          info("Received an error response from the subscriber (seq={}). ", e)
        }
        case None => {
          info("Received an publish response from the subscriber (seq={}).", sequence)
          this ! Ack(sequence)
        }
      }
    }

    override def start() = {
      super.start()
      if (publishTPS > 0) {
        publishScheduler.start()
      }
      this
    }

    def act() {
      loop {
        react {
          case PublishNext => {
            try {
              if (source.hasNext && canPublishMore) {
                source.next() match {
                  case Some(txMessage) => {
                    val sequence = nextSequence
                    // Must fail if timestamp is missing
                    val timestamp = Consistency.getMessageTimestamp(txMessage).get
                    val params: Seq[(String, MValue with Product with Serializable)] =
                      Seq((Sequence -> sequence), (SubscriptionId -> subscriptionId),
                        (ReplicationParam.Timestamp -> timestamp.value))
                    val publishMessage = new OutMessage(params = params, data = txMessage,
                      onReply = onPublishReply(sequence), responseTimeout = service.responseTimeout)
                    publishMessage.destination = new Endpoints(Seq(new Shard(-1, Seq(new Replica(-1, subscriber)))))

                    info("Publishing message to subscriber {} (seq={}).", txMessage, sequence)

                    publishAction.call(publishMessage)
                    pendingSequences += sequence
                  }
                  case None => // No more message available at this time
                }
              }

            } catch {
              case e: Exception => {
                info("Error publishing a transaction.", e)
                // TODO: cancel subscription
              }
            } finally {
              reply(true)
            }
          }
          case Ack(sequence) => {
            try {
              pendingSequences -= sequence
            } catch {
              case e: Exception => {
                info("Error acknoledging transaction {}.", sequence, e)
                // TODO: cancel subscription
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
                info("Error kiiling actor.", e)
              }
            } finally {
              reply(true)
            }
          }
        }
      }
    }
  }

}
