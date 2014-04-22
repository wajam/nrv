package com.wajam.nrv.consistency

import com.wajam.nrv.utils.timestamp.Timestamp
import com.wajam.nrv.data.{MBoolean, Message}
import com.wajam.nrv.service._
import java.util.concurrent.ConcurrentSkipListSet
import com.wajam.commons.{Logging, Closable}
import scala.concurrent.{ExecutionContext, Future}
import com.wajam.nrv.consistency.TransactionStorage.LookupKey

/**
 * Dummy consistent store service used to test ConsistencyMasterSlave
 */
class DummyConsistentStoreService(name: String, val localStorage: TransactionStorage, replicasCount: Int = 1)
  extends Service(name, new ActionSupportOptions(
    resolver = Some(new Resolver(replicasCount, tokenExtractor = Resolver.TOKEN_PARAM("token"))),
    responseTimeout = Some(3000L)
  ))
  with ConsistentStore
  with Logging {

  private var currentConsistentTimestamp: Option[(TokenRange) => Timestamp] = None

  private val addAction = registerAction(new Action("/execute/:token", req => {
    info(s"ADD: tk=${req.token}, ts=${req.timestamp.get}, data=${req.getData[String]}, node=${cluster.localNode}")
    req.parameters.get("no_reply") match {
      case Some(noReply: MBoolean) if noReply.value => // Don't reply!
      case _ => req.reply(null, data = localStorage.add(req).toString )
    }
  }, ActionMethod.POST))

  private val getAction = registerAction(new Action("/execute/:token", req => {
    info(s"GET: tk=${req.token}, ts=${req.getData[String]}, node=${cluster.localNode}")
    req.reply(null, data = localStorage.getValue(req.getData[String].toLong).get)
  }, ActionMethod.GET))

  def addRemoteValue(value: String, mustReply: Boolean = true)(implicit ec: ExecutionContext): Future[LookupKey] = {
    val token = Resolver.hashData(value)
    val response = addAction.call(params = Seq("token" -> token, "no_reply" -> !mustReply), meta = Seq(), data = value)
    response.map(msg => LookupKey(token, msg.getData[String].toLong))
  }

  def getRemoteValue(key: LookupKey)(implicit ec: ExecutionContext): Future[String] = {
    val response = getAction.call(params = Seq("token" -> key.token), meta = Seq(), data = key.timestamp.toString)
    response.map(msg => msg.getData[String])
  }

  def getCurrentConsistentTimestamp(range: TokenRange): Option[Timestamp] = {
    currentConsistentTimestamp.map(_(range))
  }

  // ConsistentStore trait implementation

  def invalidateCache() = {}

  /**
   * Returns true of the specified message must be handled (e.g. timestamped, written in transaction log and
   * replicated) by the consistency manager.
   */
  def requiresConsistency(message: Message) = {
    findAction(message.path, message.method).exists(action => action == addAction || action == getAction)
  }

  /**
   * Returns the latest record timestamp for the specified token ranges
   */
  def getLastTimestamp(ranges: Seq[TokenRange]) = {
    val lastTs = localStorage.reverseIterator.find(msg => ranges.exists(range => range.contains(msg.token))).flatMap(m => m.timestamp)
    trace(s"getLastTimestamp: $lastTs, $ranges, ${localStorage.iterator.map(_.getData[String]).toSeq}")
    lastTs
  }

  /**
   * Setup the function which returns the most recent timestamp considered as consistent by the Consistency manager
   * for the specified token range. The consistency of the records more recent that the consistent timestamp is
   * unconfirmed and these records must be excluded from processing tasks such as GC or percolation.
   */
  def setCurrentConsistentTimestamp(getCurrentConsistentTimestamp: (TokenRange) => Timestamp) = {
    currentConsistentTimestamp = Some(getCurrentConsistentTimestamp)
  }

  /**
   * Returns the mutation messages from and up to the given timestamps inclusively for the specified token ranges.
   */
  def readTransactions(fromTime: Timestamp, toTime: Timestamp, ranges: Seq[TokenRange]) = {
    val from = Some(fromTime)
    val to = Some(toTime)
    val itr = localStorage.iterator.collect {
      case msg if ranges.exists(range => range.contains(msg.token)) && msg.timestamp >= from && msg.timestamp <= to => {
        msg
      }
    }

    new Iterator[Message] with Closable {
      def hasNext = itr.hasNext

      def next() = itr.next()

      def close() = {}
    }
  }

  /**
   * Apply the specified mutation message to this consistent database
   */
  def writeTransaction(message: Message) = {
    localStorage.add(message)
  }

  /**
   * Truncate all records at the given timestamp for the specified token.
   */
  def truncateAt(timestamp: Timestamp, token: Long) = {
    localStorage.remove(timestamp)
  }
}

/**
 * Stores the local transactions.
 */
class TransactionStorage {

  private val transactions = new ConcurrentSkipListSet[Message](TransactionStorage.Ordering)

  def add(message: Message): Timestamp = {
    val ts = message.timestamp.get
    transactions.add(message)
    ts
  }

  def remove(timestamp: Timestamp): Option[Message] = {
    val msg = get(timestamp)
    msg.foreach(transactions.remove)
    msg
  }

  def get(timestamp: Timestamp): Option[Message] = {
    val ts = Some(timestamp)
    reverseIterator.find(msg => msg.timestamp == ts)
  }

  def getValue(timestamp: Timestamp): Option[String] = get(timestamp).map(msg => msg.getData[String])

  import collection.JavaConversions._

  def iterator: Iterator[Message] = transactions.iterator()

  def reverseIterator: Iterator[Message] = transactions.descendingIterator()

}

object TransactionStorage {
  case class LookupKey(token: Long, timestamp: Timestamp)

  val Ordering = new Ordering[Message] {
    def compare(x: Message, y: Message) = {
      x.timestamp.compareTo(y.timestamp)
    }
  }

}
