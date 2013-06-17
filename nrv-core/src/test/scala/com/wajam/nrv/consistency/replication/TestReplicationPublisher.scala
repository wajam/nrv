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
import com.wajam.nrv.consistency.persistence.LogRecord.{Response, Request, Index}
import org.mockito.ArgumentCaptor
import scala.collection.JavaConversions._
import com.wajam.nrv.utils.Closable

@RunWith(classOf[JUnitRunner])
class TestReplicationPublisher extends TestTransactionBase with BeforeAndAfter with MockitoSugar {

  var service: Service = null
  var member: ResolvedServiceMember = null
  var mockStore: ConsistentStore = null
  var mockPublishAction: Action = null

  var subscriptionId: String = "id"
  var currentConsistentTimestamp: Option[Timestamp] = None
  var publisher: ReplicationPublisher = null

  var publishWindowSize = 2
  val subscriptionTimeout = 1500L
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

  def subscribe(startTimestamp: Option[Timestamp], mode: ReplicationMode): ReplicationSubscription = {
    val cookie = "cookie"

    var subscribeRequest: Map[String, MValue] = Map(
      ReplicationParam.Token -> token.toString,
      ReplicationParam.Cookie -> cookie,
      ReplicationParam.Mode -> mode.toString)
    startTimestamp.foreach(ts => subscribeRequest += ReplicationParam.Start -> ts.value)

    val subscribeRequestMessage = new InMessage(subscribeRequest)
    var subscribeResponseMessage: Option[OutMessage] = None
    subscribeRequestMessage.replyCallback = (reply) => subscribeResponseMessage = Some(reply)

    publisher.handleSubscribeMessage(subscribeRequestMessage)
    val subscription = publisher.subscriptions.head
    subscription.member should be(member)
    subscription.cookie should be(cookie)
    subscription.id should be(Some(subscriptionId))
    // Do not verify subscription mode, start and end timestamps. These values can be different from the ones requested
    // if the publisher fallback to a different mode.
    subscribeResponseMessage.get.error should be(None)

    subscription
  }

  /**
   * Generates a list of synthetic transaction records (Request + Response log records) finalized by an Index record.
   */
  def createTransactions(count: Int, initialTimestamp: Long = 0, timestampIncrement: Int = 1): List[LogRecord] = {

    def create(nextId: Long, timestamp: Long, consistentTimestamp: Option[Timestamp], remaining: Int): List[LogRecord] = {
      if (remaining > 0) {
        val message = createRequestMessage(timestamp)
        val request = Request(nextId, consistentTimestamp, message)
        val response = Response(request.id + 1, consistentTimestamp, createResponseMessage(message))
        println(request)
        println(response)
        request :: response :: create(response.id + 1, timestamp + timestampIncrement, Some(timestamp), remaining - 1)
      } else {
        val index = Index(nextId, consistentTimestamp)
        println(index)
        index :: Nil
      }
    }

    create(0, initialTimestamp, None, count)
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


  def assertPublishMessageEquals(actual: Message, expected: Message) {
    actual.parameters.toMap should be(expected.parameters.toMap)
    actual.getData[Message].parameters should be(expected.getData[Message].parameters)
  }

  def toStoreIterator(records: Seq[LogRecord], startTimestamp: Long): Iterator[Message] with Closable = {
    // Only keep Request records. Note that the start timestamp is exclusive.
    val requests = records.collect {
      case request: Request if request.timestamp > startTimestamp => request
    }

    new Iterator[Message] with Closable {
      val itr = requests.map(_.message).toIterator
      var closed = false

      def hasNext = itr.hasNext

      def next() = itr.next()

      def close() {
        closed = true
      }
    }
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

  test("subscribe live mode should fail if master has no transaction log") {
    val subscribeRequest: Map[String, MValue] = Map(
      ReplicationParam.Token -> token.toString,
      ReplicationParam.Cookie -> "cookie",
      ReplicationParam.Start -> "12345",
      ReplicationParam.Mode -> ReplicationMode.Live.toString)

    val subscribeRequestMessage = new InMessage(subscribeRequest)
    var subscribeResponseMessage: Option[OutMessage] = None
    subscribeRequestMessage.replyCallback = (reply) => subscribeResponseMessage = Some(reply)

    txLog.read.toList should be(Nil)

    publisher.handleSubscribeMessage(subscribeRequestMessage)
    publisher.subscriptions should be(Nil) // synchronized actor message
    subscribeResponseMessage.get.error should not be None
    verifyZeroInteractionsAfter(wait = 100, mockPublishAction)
  }

  test("subscribe live mode should publish expected transactions up to window size") {
    val logRecords = createTransactions(count = 10, initialTimestamp = 0)
    logRecords.foreach(txLog.append(_))
    logRecords should be(txLog.read.toList)
    currentConsistentTimestamp = logRecords.last.consistentTimestamp

    val startTimestamp = 1L
    val subscription = subscribe(Some(startTimestamp), ReplicationMode.Live)
    subscription.mode should be(ReplicationMode.Live)
    subscription.startTimestamp should be(Some(Timestamp(startTimestamp)))
    subscription.endTimestamp should be(None)

    val publishCaptor = ArgumentCaptor.forClass(classOf[OutMessage])
    verify(mockPublishAction, timeout(1000).atLeast(publishWindowSize)).callOutgoingHandlers(publishCaptor.capture())

    // Verify received expected messages up to the window size
    val expectedPublished = toPublishMessages(logRecords, startTimestamp)
    val actualPublished = publishCaptor.getAllValues.toList
    expectedPublished.zip(actualPublished).foreach {
      case (expected, actual) => assertPublishMessageEquals(actual = actual, expected = expected)
    }
    actualPublished.size should be(publishWindowSize)
    verifyNoMoreInteractionsAfter(wait = 100, mockPublishAction)
  }

  test("replying first publish after full window size should publish new transactions") {
    val logRecords = createTransactions(count = 10, initialTimestamp = 0)
    logRecords.foreach(txLog.append(_))
    logRecords should be(txLog.read.toList)
    currentConsistentTimestamp = logRecords.last.consistentTimestamp

    val startTimestamp = 1L
    val subscription = subscribe(Some(startTimestamp), ReplicationMode.Live)
    subscription.mode should be(ReplicationMode.Live)
    subscription.startTimestamp should be(Some(Timestamp(startTimestamp)))
    subscription.endTimestamp should be(None)

    val publishCaptor = ArgumentCaptor.forClass(classOf[OutMessage])
    verify(mockPublishAction, timeout(1000).atLeast(publishWindowSize)).callOutgoingHandlers(publishCaptor.capture())

    // Verify received expected messages up to the window size
    val expectedPublished = toPublishMessages(logRecords, startTimestamp)
    val actualPublished = publishCaptor.getAllValues.toList
    expectedPublished.zip(actualPublished).foreach {
      case (expected, actual) => assertPublishMessageEquals(actual = actual, expected = expected)
    }
    actualPublished.size should be(publishWindowSize)
    verifyNoMoreInteractionsAfter(wait = 100, mockPublishAction)

    // Verify a new message is published if first received message is replied
    reset(mockPublishAction)
    actualPublished.head.handleReply(new InMessage())
    verify(mockPublishAction, timeout(1000).times(1)).callOutgoingHandlers(publishCaptor.capture())
    assertPublishMessageEquals(actual = publishCaptor.getValue, expected = expectedPublished(publishWindowSize))
    verifyNoMoreInteractionsAfter(wait = 100, mockPublishAction)
  }

  test("replying last publish after full window size should NOT publish new transactions") {
    val logRecords = createTransactions(count = 10, initialTimestamp = 0)
    logRecords.foreach(txLog.append(_))
    logRecords should be(txLog.read.toList)
    currentConsistentTimestamp = logRecords.last.consistentTimestamp

    val startTimestamp = 1L
    val subscription = subscribe(Some(startTimestamp), ReplicationMode.Live)
    subscription.mode should be(ReplicationMode.Live)
    subscription.startTimestamp should be(Some(Timestamp(startTimestamp)))
    subscription.endTimestamp should be(None)

    val publishCaptor = ArgumentCaptor.forClass(classOf[OutMessage])
    verify(mockPublishAction, timeout(1000).atLeast(publishWindowSize)).callOutgoingHandlers(publishCaptor.capture())

    // Verify received expected messages up to the window size
    val expectedPublished = toPublishMessages(logRecords, startTimestamp)
    val actualPublished = publishCaptor.getAllValues.toList
    expectedPublished.zip(actualPublished).foreach {
      case (expected, actual) => assertPublishMessageEquals(actual = actual, expected = expected)
    }
    actualPublished.size should be(publishWindowSize)
    verifyNoMoreInteractionsAfter(wait = 100, mockPublishAction)

    // Verify NO new message is published if last received message is replied
    reset(mockPublishAction)
    actualPublished.last.handleReply(new InMessage())
    verifyZeroInteractionsAfter(wait = 500, mockPublishAction)
  }

  test("subscribe live mode should fallback to store mode if no start timestamp is specified") {
    val logRecords = createTransactions(count = 10, initialTimestamp = 0)
    logRecords.foreach(txLog.append(_))
    logRecords should be(txLog.read.toList)
    currentConsistentTimestamp = logRecords.last.consistentTimestamp

    val startTimestamp = Long.MinValue
    when(mockStore.readTransactions(startTimestamp, currentConsistentTimestamp.get, member.ranges)).thenReturn(
      toStoreIterator(logRecords, startTimestamp))

    // Subscribe without specifying a start timestamp
    val subscription = subscribe(startTimestamp = None, ReplicationMode.Live)
    subscription.mode should be(ReplicationMode.Store)
    subscription.startTimestamp should be(Some(Timestamp(startTimestamp)))
    subscription.endTimestamp should be(currentConsistentTimestamp)

    val publishCaptor = ArgumentCaptor.forClass(classOf[OutMessage])
    verify(mockPublishAction, timeout(1000).atLeast(publishWindowSize)).callOutgoingHandlers(publishCaptor.capture())

    // Verify received expected messages up to the window size
    val expectedPublished = toPublishMessages(logRecords, Long.MinValue)
    val actualPublished = publishCaptor.getAllValues.toList
    expectedPublished.zip(actualPublished).foreach {
      case (expected, actual) => assertPublishMessageEquals(actual = actual, expected = expected)
    }
    actualPublished.size should be(publishWindowSize)
    verifyNoMoreInteractionsAfter(wait = 100, mockPublishAction)
  }

  test("subscribe live mode should fallback to store mode if start timestamp before first log timestamp") {
    val logRecords = createTransactions(count = 10, initialTimestamp = 0)
    // Append only second half of the transactions in log, first transactions will come from the store
    logRecords.slice(logRecords.size / 2, logRecords.size).foreach(txLog.append(_))
    currentConsistentTimestamp = logRecords.last.consistentTimestamp

    val startTimestamp = 1L
    when(mockStore.readTransactions(startTimestamp, currentConsistentTimestamp.get, member.ranges)).thenReturn(
      toStoreIterator(logRecords, startTimestamp))

    val subscription = subscribe(Some(startTimestamp), ReplicationMode.Live)
    subscription.mode should be(ReplicationMode.Store)
    subscription.startTimestamp should be(Some(Timestamp(startTimestamp)))
    subscription.endTimestamp should be(currentConsistentTimestamp)

    val publishCaptor = ArgumentCaptor.forClass(classOf[OutMessage])
    verify(mockPublishAction, timeout(1000).atLeast(publishWindowSize)).callOutgoingHandlers(publishCaptor.capture())

    // Verify received expected messages up to the window size
    val expectedPublished = toPublishMessages(logRecords, startTimestamp)
    val actualPublished = publishCaptor.getAllValues.toList
    expectedPublished.zip(actualPublished).foreach {
      case (expected, actual) => assertPublishMessageEquals(actual = actual, expected = expected)
    }
    actualPublished.size should be(publishWindowSize)
    verifyNoMoreInteractionsAfter(wait = 100, mockPublishAction)
  }

  test("subscribe store mode should publish expected transactions up to window size") {
    val logRecords = createTransactions(count = 10, initialTimestamp = 0)
    // Append only second half of the transactions in log, first transaction will come from the store
    logRecords.slice(logRecords.size / 2, logRecords.size).foreach(txLog.append(_))
    currentConsistentTimestamp = logRecords.last.consistentTimestamp

    val startTimestamp = 1L
    when(mockStore.readTransactions(startTimestamp, currentConsistentTimestamp.get, member.ranges)).thenReturn(
      toStoreIterator(logRecords, startTimestamp))

    val subscription = subscribe(Some(startTimestamp), ReplicationMode.Store)
    subscription.mode should be(ReplicationMode.Store)
    subscription.startTimestamp should be(Some(Timestamp(startTimestamp)))
    subscription.endTimestamp should be(currentConsistentTimestamp)

    val publishCaptor = ArgumentCaptor.forClass(classOf[OutMessage])
    verify(mockPublishAction, timeout(1000).atLeast(publishWindowSize)).callOutgoingHandlers(publishCaptor.capture())

    // Verify received expected messages up to the window size
    val expectedPublished = toPublishMessages(logRecords, startTimestamp)
    val actualPublished = publishCaptor.getAllValues.toList
    expectedPublished.zip(actualPublished).foreach {
      case (expected, actual) => assertPublishMessageEquals(actual = actual, expected = expected)
    }
    actualPublished.size should be(publishWindowSize)
    verifyNoMoreInteractionsAfter(wait = 100, mockPublishAction)
  }

  test("subscribe store mode without log should fail") {
    val logRecords = createTransactions(count = 10, initialTimestamp = 0)
    currentConsistentTimestamp = logRecords.last.consistentTimestamp
    val startTimestamp = 1L
    when(mockStore.readTransactions(startTimestamp, currentConsistentTimestamp.get, member.ranges)).thenReturn(
      toStoreIterator(logRecords, startTimestamp))

    val subscribeRequest: Map[String, MValue] = Map(
      ReplicationParam.Token -> token.toString,
      ReplicationParam.Cookie -> "cookie",
      ReplicationParam.Start -> startTimestamp,
      ReplicationParam.Mode -> ReplicationMode.Store.toString)

    val subscribeRequestMessage = new InMessage(subscribeRequest)
    var subscribeResponseMessage: Option[OutMessage] = None
    subscribeRequestMessage.replyCallback = (reply) => subscribeResponseMessage = Some(reply)

    publisher.handleSubscribeMessage(subscribeRequestMessage)
    publisher.subscriptions should be(Nil) // synchronized actor message
    subscribeResponseMessage.get.error should not be None
    verifyZeroInteractions(mockPublishAction)
  }

  test("store mode should end subscription when reaching end timestamp") {
    val logRecords = createTransactions(count = 10, initialTimestamp = 0)
    logRecords.foreach(txLog.append(_))
    currentConsistentTimestamp = logRecords.last.consistentTimestamp
    publishWindowSize = 100

    val startTimestamp = 1L
    when(mockStore.readTransactions(startTimestamp, currentConsistentTimestamp.get, member.ranges)).thenReturn(
      toStoreIterator(logRecords, startTimestamp))

    val subscription = subscribe(Some(startTimestamp), ReplicationMode.Store)
    subscription.mode should be(ReplicationMode.Store)
    subscription.startTimestamp should be(Some(Timestamp(startTimestamp)))
    subscription.endTimestamp should be(currentConsistentTimestamp)

    val expectedPublished = toPublishMessages(logRecords, startTimestamp)

    val publishCaptor = ArgumentCaptor.forClass(classOf[OutMessage])
    verify(mockPublishAction, timeout(1000).atLeast(expectedPublished.size)).callOutgoingHandlers(publishCaptor.capture())

    // Verify received all expected messages
    val actualPublished = publishCaptor.getAllValues.toList
    expectedPublished.zip(actualPublished).foreach {
      case (expected, actual) => assertPublishMessageEquals(actual = actual, expected = expected)
    }
    actualPublished.size should be(expectedPublished.size)
    publisher.subscriptions should be(Nil) // Subscription should be terminated after reaching end timestamp
    verifyNoMoreInteractionsAfter(wait = 100, mockPublishAction)
  }

  ignore("live mode should end subscription when reaching end of log file") {

  }

  ignore("live mode should publish idle message when reaching consistent timestamp") {

  }

  ignore("live mode should publish more transactions when consistent timestamp increase") {

  }

  ignore("live mode should publish new transactions when appended in log and consistent timestamp increase") {

  }

  test("unsubscribe should kill subscription") {
    val logRecords = createTransactions(count = 10, initialTimestamp = 0)
    logRecords.foreach(txLog.append(_))
    currentConsistentTimestamp = logRecords.last.consistentTimestamp

    val startTimestamp = 1L
    val subscription = subscribe(Some(startTimestamp), ReplicationMode.Live)
    subscription.mode should be(ReplicationMode.Live)
    subscription.startTimestamp should be(Some(Timestamp(startTimestamp)))
    subscription.endTimestamp should be(None)
    publisher.subscriptions should be(List(subscription))

    val unsubscribeRequest: Map[String, MValue] = Map(
      ReplicationParam.SubscriptionId -> subscription.id.get)
    val unsubscribeRequestMessage = new InMessage(unsubscribeRequest)
    var unsubscribeResponseMessage: Option[OutMessage] = None
    unsubscribeRequestMessage.replyCallback = (reply) => unsubscribeResponseMessage = Some(reply)

    publisher.handleUnsubscribeMessage(unsubscribeRequestMessage)
    publisher.subscriptions should be(Nil)
    unsubscribeResponseMessage.get.error should be(None)
    verifyZeroInteractionsAfter(wait = 100, mockPublishAction)
  }

  test("unsubscribe unknown subscription should do nothing") {
    val logRecords = createTransactions(count = 10, initialTimestamp = 0)
    logRecords.foreach(txLog.append(_))
    currentConsistentTimestamp = logRecords.last.consistentTimestamp

    val startTimestamp = 1L
    val subscription = subscribe(Some(startTimestamp), ReplicationMode.Live)
    subscription.mode should be(ReplicationMode.Live)
    subscription.startTimestamp should be(Some(Timestamp(startTimestamp)))
    subscription.endTimestamp should be(None)
    publisher.subscriptions should be(List(subscription))

    val unsubscribeRequest: Map[String, MValue] = Map(
      ReplicationParam.SubscriptionId -> "bad id")
    val unsubscribeRequestMessage = new InMessage(unsubscribeRequest)
    var unsubscribeResponseMessage: Option[OutMessage] = None
    unsubscribeRequestMessage.replyCallback = (reply) => unsubscribeResponseMessage = Some(reply)

    publisher.handleUnsubscribeMessage(unsubscribeRequestMessage)
    publisher.subscriptions should be(List(subscription))
    unsubscribeResponseMessage.get.error should be(None)
  }

  test("terminate member subscriptions should skill all member subscriptions") {
    val logRecords = createTransactions(count = 10, initialTimestamp = 0)
    logRecords.foreach(txLog.append(_))
    currentConsistentTimestamp = logRecords.last.consistentTimestamp

    val startTimestamp = 1L
    val subscription1 = subscribe(Some(startTimestamp), ReplicationMode.Live)
    val subscription2 = subscribe(Some(startTimestamp), ReplicationMode.Live)
    publisher.subscriptions should be(List(subscription1, subscription2))

    publisher.terminateMemberSubscriptions(member)
    publisher.subscriptions should be(Nil)
  }
}
