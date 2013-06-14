package com.wajam.nrv.consistency.replication

import org.scalatest.BeforeAndAfter
import org.scalatest.mock.MockitoSugar
import org.scalatest.matchers.ShouldMatchers._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import com.wajam.nrv.service._
import com.wajam.nrv.service.ActionProxy._
import com.wajam.nrv.consistency.{TestTransactionBase, ResolvedServiceMember, ConsistentStore}
import com.wajam.nrv.data._
import com.wajam.nrv.cluster.{Cluster, StaticClusterManager, Node, LocalNode}
import java.io.File
import com.wajam.nrv.consistency.persistence.{LogRecord, FileTransactionLog}
import java.nio.file.Files
import com.wajam.nrv.utils.timestamp.Timestamp
import org.mockito.Mockito._
import org.mockito.Matchers._
import com.wajam.nrv.data.MessageMatcher._
import com.wajam.nrv.consistency.persistence.LogRecord.{Request, Index}
import scala.annotation.tailrec
import org.mockito.ArgumentCaptor
import scala.collection.JavaConversions._
import com.wajam.nrv.consistency.persistence.LogRecord.Index
import scala.Some

@RunWith(classOf[JUnitRunner])
class TestReplicationPublisher extends TestTransactionBase with BeforeAndAfter with MockitoSugar {

  var service: Service = null
  var member: ResolvedServiceMember = null
  var mockStore: ConsistentStore = null
  var mockPublishAction: Action = null

  var subscriptionId: String = "id"
  var currentConsistentTimestamp: Option[Timestamp] = None
  var publisher: ReplicationPublisher = null

  val subscriptionTimeout = 1500L
  val publishWindowSize = 2
  val token = 0
  val remoteToken: Long = Int.MaxValue

  val localNode = new LocalNode("127.0.0.1", Map("nrv" -> 12345))
  val remoteNode = new Node("127.0.0.2", Map("nrv" -> 54321))

  var logDir: File = null
  var txLog: FileTransactionLog = null

  before {
    service = new Service("service")
    val cluster = new Cluster(localNode, new StaticClusterManager)
    cluster.registerService(service)
    service.addMember(new ServiceMember(token, localNode))
    service.addMember(new ServiceMember(remoteToken, remoteNode))

    member = ResolvedServiceMember(service, token)

    logDir = Files.createTempDirectory("TestReplicationPublisher").toFile
    txLog = new FileTransactionLog(member.serviceName, member.token, logDir = logDir.getAbsolutePath)

    mockStore = mock[ConsistentStore]
    mockPublishAction = mock[Action]

    publisher = new ReplicationPublisher(service, mockStore, (_) => txLog, (_) => currentConsistentTimestamp,
      ActionProxy(mockPublishAction), publishTps = 100, publishWindowSize, subscriptionTimeout) {
      override def nextId = subscriptionId
    }
    publisher.start()
  }

  after {
    publisher.stop()
    publisher = null
    mockPublishAction = null
    mockStore = null

    txLog.close()
    txLog = null

    logDir.listFiles().foreach(_.delete())
    logDir.delete()
    logDir = null

    currentConsistentTimestamp = None
  }

  /**
   * Generates synthetic transaction records (Request + Response) and append them to the transaction log.
   * The transaction log is finalized by an Index record. Returns the appended log records.
   * TODO: do not returns records, simply read them from tx log
   */
  def appendTransactions(count: Int, initialTimestamp: Long = 0, timestampIncrement: Int = 1) {

    @tailrec
    def append(nextId: Long, timestamp: Long, consistentTimestamp: Option[Timestamp], remaining: Int) {
      if (remaining > 0) {
        val message = createRequestMessage(timestamp)
        val request = txLog.append(LogRecord(nextId, consistentTimestamp, message))
        val response = txLog.append(LogRecord(request.id + 1, consistentTimestamp, createResponseMessage(message)))
        println(request)
        println(response)
        append(response.id + 1, timestamp + timestampIncrement, Some(timestamp), remaining - 1)
      } else {
        val index = txLog.append(Index(nextId, consistentTimestamp))
        println(index)
      }
    }

    append(0, initialTimestamp, None, count)
  }

  test("subscribe should fail if publisher is not master of the service member") {
    val subscribeRequest: Map[String, MValue] = Map(
      ReplicationParam.Token -> remoteToken.toString,
      ReplicationParam.Cookie -> "cookie",
      ReplicationParam.Start -> "12345",
      ReplicationParam.Mode -> ReplicationMode.Store.toString)

    val subscribeRequestMessage = new InMessage(subscribeRequest)
    var subscribeResponseMessage: Option[OutMessage] = None
    subscribeRequestMessage.replyCallback = (reply) => subscribeResponseMessage = Some(reply)

    publisher.handleSubscribeMessage(subscribeRequestMessage)
    publisher.subscriptions should be(Nil) // synchronized actor message
    subscribeResponseMessage.get.error should not be None
    verifyZeroInteractions(mockPublishAction)
  }

  test("subscribe should fail if service member does not exist") {
    val subscribeRequest: Map[String, MValue] = Map(
      ReplicationParam.Token -> "1000",
      ReplicationParam.Cookie -> "cookie",
      ReplicationParam.Start -> "12345",
      ReplicationParam.Mode -> ReplicationMode.Store.toString)

    val subscribeRequestMessage = new InMessage(subscribeRequest)
    var subscribeResponseMessage: Option[OutMessage] = None
    subscribeRequestMessage.replyCallback = (reply) => subscribeResponseMessage = Some(reply)

    publisher.handleSubscribeMessage(subscribeRequestMessage)
    publisher.subscriptions should be(Nil) // synchronized actor message
    subscribeResponseMessage.get.error should not be None
    verifyZeroInteractions(mockPublishAction)
  }

  def toPublishMessages(records: Seq[LogRecord], startTimestamp: Long): Seq[Message] = {
    // Only keep Request records. Note that the start timestamp is exclusive.
    val requests = records.collect {
      case request: Request if request.timestamp > startTimestamp => request
    }

    requests.zipWithIndex.map {
      case (request, i) => {
        val publishParams: Map[String, MValue] = Map(
          ReplicationParam.Timestamp -> request.timestamp.value,
          ReplicationParam.SubscriptionId -> subscriptionId,
          ReplicationParam.Sequence -> (i + 1).toLong)
        new OutMessage(publishParams, data = request.message)
      }
    }
  }

  test("subscribe live mode should fail if master has no transaction log") {
    val subscribeRequest: Map[String, MValue] = Map(
      ReplicationParam.Token -> token.toString,
      ReplicationParam.Cookie -> "cookie",
      ReplicationParam.Start -> "12345",
      ReplicationParam.Mode -> ReplicationMode.Live.toString)

    val subscribeRequestMessage = new InMessage(subscribeRequest)
    var subscribeResponseMessage: Option[OutMessage] = None
    subscribeRequestMessage.replyCallback = (reply) => subscribeResponseMessage = Some(reply)

    txLog.read.toList should be (Nil)

    publisher.handleSubscribeMessage(subscribeRequestMessage)
    publisher.subscriptions should be(Nil) // synchronized actor message
    subscribeResponseMessage.get.error should not be None
    verifyZeroInteractionsAfter(wait = 100, mockPublishAction)
  }

  def assertPublishMessageEquals(actual: Message, expected: Message) {
    actual.parameters.toMap should be (expected.parameters.toMap)
    actual.getData[Message].parameters should be (expected.getData[Message].parameters)
  }

  def subscribe(startTimestamp: Option[Timestamp], mode: ReplicationMode): ReplicationSubscription = {
    val cookie = "cookie"
    val expectedSubscription = ReplicationSubscription(member, cookie, ReplicationMode.Live,
      Some(subscriptionId), startTimestamp)

    // Subscribe
    var subscribeRequest: Map[String, MValue] = Map(
      ReplicationParam.Token -> token.toString,
      ReplicationParam.Cookie -> cookie,
      ReplicationParam.Mode -> ReplicationMode.Live.toString)
    startTimestamp.foreach(ts => subscribeRequest += ReplicationParam.Start -> ts.value)

    val subscribeRequestMessage = new InMessage(subscribeRequest)
    var subscribeResponseMessage: Option[OutMessage] = None
    subscribeRequestMessage.replyCallback = (reply) => subscribeResponseMessage = Some(reply)

    publisher.handleSubscribeMessage(subscribeRequestMessage)
    publisher.subscriptions should be(List(expectedSubscription)) // synchronized actor message
    subscribeResponseMessage.get.error should be(None)

    publisher.subscriptions.head
  }

  test("subscribe live mode should publish expected transactions up to window size") {
    appendTransactions(count = 10, initialTimestamp = 0)
    val logRecords = txLog.read.toList
    currentConsistentTimestamp = logRecords.last.consistentTimestamp

    val startTimestamp = 1L
    val subscription = subscribe(Some(startTimestamp), ReplicationMode.Live)

    val publishCaptor = ArgumentCaptor.forClass(classOf[OutMessage])
    verify(mockPublishAction, timeout(1000).atLeast(publishWindowSize)).callOutgoingHandlers(publishCaptor.capture())

    // Verify received expected messages up to the window size
    val expectedPublished = toPublishMessages(logRecords, startTimestamp)
    val actualPublished = publishCaptor.getAllValues.toList
    expectedPublished.zip(actualPublished).foreach{case (expected, actual) => {
      assertPublishMessageEquals(actual = actual, expected = expected)
    }}
    actualPublished.size should be (publishWindowSize)
    verifyNoMoreInteractionsAfter(wait = 100, mockPublishAction)
  }

  test("replying first publish after full window size should publish new transactions") {
    appendTransactions(count = 10, initialTimestamp = 0)
    val logRecords = txLog.read.toList
    currentConsistentTimestamp = logRecords.last.consistentTimestamp

    val startTimestamp = 1L
    val subscription = subscribe(Some(startTimestamp), ReplicationMode.Live)

    val publishCaptor = ArgumentCaptor.forClass(classOf[OutMessage])
    verify(mockPublishAction, timeout(1000).atLeast(publishWindowSize)).callOutgoingHandlers(publishCaptor.capture())

    // Verify received expected messages up to the window size
    val expectedPublished = toPublishMessages(logRecords, startTimestamp)
    val actualPublished = publishCaptor.getAllValues.toList
    expectedPublished.zip(actualPublished).foreach{case (expected, actual) => {
      assertPublishMessageEquals(actual = actual, expected = expected)
    }}
    actualPublished.size should be (publishWindowSize)
    verifyNoMoreInteractionsAfter(wait = 100, mockPublishAction)

    // Verify a new message is published if first received message is replied
    reset(mockPublishAction)
    actualPublished.head.handleReply(new InMessage())
    verify(mockPublishAction, timeout(1000).times(1)).callOutgoingHandlers(publishCaptor.capture())
    assertPublishMessageEquals(actual = publishCaptor.getValue, expected = expectedPublished(publishWindowSize))
    verifyNoMoreInteractionsAfter(wait = 100, mockPublishAction)
  }

  test("replying last publish after full window size should NOT publish new transactions") {
    appendTransactions(count = 10, initialTimestamp = 0)
    val logRecords = txLog.read.toList
    currentConsistentTimestamp = logRecords.last.consistentTimestamp

    val startTimestamp = 1L
    val subscription = subscribe(Some(startTimestamp), ReplicationMode.Live)

    val publishCaptor = ArgumentCaptor.forClass(classOf[OutMessage])
    verify(mockPublishAction, timeout(1000).atLeast(publishWindowSize)).callOutgoingHandlers(publishCaptor.capture())

    // Verify received expected messages up to the window size
    val expectedPublished = toPublishMessages(logRecords, startTimestamp)
    val actualPublished = publishCaptor.getAllValues.toList
    expectedPublished.zip(actualPublished).foreach{case (expected, actual) => {
      assertPublishMessageEquals(actual = actual, expected = expected)
    }}
    actualPublished.size should be (publishWindowSize)
    verifyNoMoreInteractionsAfter(wait = 100, mockPublishAction)

    // Verify NO new message is published if last received message is replied
    reset(mockPublishAction)
    actualPublished.last.handleReply(new InMessage())
    verifyZeroInteractionsAfter(wait = 500, mockPublishAction)
  }

  test("subscribe live mode should fallback to store mode if no start timestamp is specified") {
    fail("Not implemented!")
  }

  ignore("subscribe with live replication mode but log after start timestamp") {

  }

  ignore("subscribe with store replication mode") {

  }

  ignore("subscribe with store replication mode but no log") {

  }

  ignore("unsubscribe") {

  }

  ignore("unsubscribe no subscription") {

  }

  ignore("terminate member subscriptions") {

  }

  // TODO: publish
}
