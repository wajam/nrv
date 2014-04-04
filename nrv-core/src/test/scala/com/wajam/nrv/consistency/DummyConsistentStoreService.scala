package com.wajam.nrv.consistency

import com.wajam.nrv.utils.timestamp.Timestamp
import com.wajam.nrv.data.Message
import com.wajam.nrv.service._
import java.util.concurrent.ConcurrentSkipListSet
import com.wajam.commons.{Logging, Closable}
import com.wajam.nrv.protocol.codec.StringCodec
import scala.concurrent.{ExecutionContext, Future}
import com.wajam.nrv.consistency.DummyConsistentStoreService.LookupKey

/**
 * Dummy consistent store service used to test ConsistencyMasterSlave
 */
class DummyConsistentStoreService(name: String, replicasCount: Int = 1)
  extends Service(name,
    new ActionSupportOptions(resolver = Some(new Resolver(replicasCount, tokenExtractor = Resolver.TOKEN_PARAM("token")))))
  with ConsistentStore
  with Logging {

  private val transactions = new ConcurrentSkipListSet[Message](DummyConsistentStoreService.Ordering)

  private val addAction = registerAction(new Action("/execute/:token",
    req => req.reply(null, data = {
      info(s"ADD: tk=${req.token}, ts=${req.timestamp.get}, data=${req.getData[String]}, node=${cluster.localNode}")
      addLocal(req).toString
    }), ActionMethod.POST))

  private val getAction = registerAction(new Action("/execute/:token",
    req => req.reply(null, data = {
      info(s"GET: tk=${req.token}, ts=${req.getData[String]}, node=${cluster.localNode}")
      getLocalValue(req.getData[String].toLong).get
    }), ActionMethod.GET))

  def addRemoteValue(value: String)(implicit ec: ExecutionContext): Future[LookupKey] = {
    val token = Resolver.hashData(value)
    val response = addAction.call(params = Seq("token" -> token), meta = Seq(), data = value)
    response.map(msg => LookupKey(token, msg.getData[String].toLong))
  }

  def getRemoteValue(key: LookupKey)(implicit ec: ExecutionContext): Future[String] = {
    val response = getAction.call(params = Seq("token" -> key.token), meta = Seq(), data = key.timestamp.toString)
    response.map(msg => msg.getData[String])
  }
  
  def addLocal(message: Message): Timestamp = {
    val ts = message.timestamp.get
    transactions.add(message)
    ts
  }

  def removeLocal(timestamp: Timestamp): Option[Message] = {
    val msg = getLocal(timestamp)
    msg.foreach(transactions.remove)
    msg
  }

  def getLocal(timestamp: Timestamp): Option[Message] = {
    import collection.JavaConversions._

    val ts = Some(timestamp)
    transactions.iterator().find(msg => msg.timestamp == ts)
  }

  def getLocalValue(timestamp: Timestamp): Option[String] = getLocal(timestamp).map(msg => msg.getData[String])

  // ConsistentStore trait implementation

  def invalidateCache() = {}

  /**
   * Returns true of the specified message must be handled (e.g. timestamped, written in transaction log and
   * replicated) by the consistency manager.
   */
  def requiresConsistency(message: Message) = {
    findAction(message.path, message.method) match {
      case Some(action) => action == addAction || action == getAction
      case None => false
    }
  }

  /**
   * Returns the latest record timestamp for the specified token ranges
   */
  def getLastTimestamp(ranges: Seq[TokenRange]) = {
    import collection.JavaConversions._

    val lastTs = transactions.descendingIterator().find(msg => ranges.exists(range => range.contains(msg.token))).flatMap(m => m.timestamp)
    trace(s"getLastTimestamp: $lastTs, $ranges, ${transactions.map(_.getData[String])}")
    lastTs
  }

  /**
   * Setup the function which returns the most recent timestamp considered as consistent by the Consistency manager
   * for the specified token range. The consistency of the records more recent that the consistent timestamp is
   * unconfirmed and these records must be excluded from processing tasks such as GC or percolation.
   */
  def setCurrentConsistentTimestamp(getCurrentConsistentTimestamp: (TokenRange) => Timestamp) = {}

  /**
   * Returns the mutation messages from and up to the given timestamps inclusively for the specified token ranges.
   */
  def readTransactions(fromTime: Timestamp, toTime: Timestamp, ranges: Seq[TokenRange]) = {
    import collection.JavaConversions._

    val from = Some(fromTime)
    val to = Some(toTime)
    val itr = transactions.iterator().collect {
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
    addLocal(message)
  }

  /**
   * Truncate all records at the given timestamp for the specified token.
   */
  def truncateAt(timestamp: Timestamp, token: Long) = {
    removeLocal(timestamp)
  }
}

object DummyConsistentStoreService {

  case class LookupKey(token: Long, timestamp: Timestamp)

  val Ordering = new Ordering[Message] {
    def compare(x: Message, y: Message) = {
      x.timestamp.compareTo(y.timestamp)
    }
  }
}