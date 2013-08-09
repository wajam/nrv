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
import com.wajam.nrv.consistency.persistence.LogRecord.Index
import com.wajam.nrv.Logging

@RunWith(classOf[JUnitRunner])
class TestMasterReplicationSessionManager extends TestTransactionBase with BeforeAndAfter with MockitoSugar with Logging {

  var service: Service = null
  var member: ServiceMember = null
  var ranges: Seq[TokenRange] = null
  var mockStore: ConsistentStore = null
  var mockPushAction: Action = null
  var sessionManager: MasterReplicationSessionManager = null

  var remoteMember: ServiceMember = null

  val sessionId: String = "id"
  var currentConsistentTimestamp: Option[Timestamp] = None
  var replicationWindowSize = 2
  var isDraining = false

  val sessionTimeout = 2000L

  val localNode = new LocalNode("127.0.0.1", Map("nrv" -> 12345))
  val remoteNode = new Node("127.0.0.2", Map("nrv" -> 54321))

  var logDir: File = null
  var txLog: FileTransactionLog = null

  before {
    service = new Service("service")
    val cluster = new Cluster(localNode, new StaticClusterManager)
    cluster.registerService(service)
    member = new ServiceMember(0, localNode)
    remoteMember = new ServiceMember(Int.MaxValue, remoteNode)
    service.addMember(member)
    service.addMember(remoteMember)
    ranges = ResolvedServiceMember(service, member).ranges

    logDir = Files.createTempDirectory("TestReplicationPublisher").toFile
    txLog = new FileTransactionLog(service.name, member.token, logDir = logDir.getAbsolutePath)

    mockStore = mock[ConsistentStore]
    mockPushAction = mock[Action]

    sessionManager = new MasterReplicationSessionManager(service, mockStore, (_) => txLog, (_) => currentConsistentTimestamp,
      ActionProxy(mockPushAction), pushTps = 100, replicationWindowSize, sessionTimeout) {
      override def nextId = sessionId
    }
    sessionManager.start()
  }

  after {
    sessionManager.stop()
    sessionManager = null
    mockPushAction = null
    mockStore = null

    txLog.close()
    txLog = null

    logDir.listFiles().foreach(_.delete())
    logDir.delete()
    logDir = null

    currentConsistentTimestamp = None
    isDraining = false
    replicationWindowSize = 2
  }

  /**
   * Utility to create a publisher subscription
   */
  def subscribe(startTimestamp: Option[Timestamp], mode: ReplicationMode): ReplicationSession = {
    val cookie = "cookie"

    var subscribeRequest: Map[String, MValue] = Map(
      ReplicationAPIParams.Token -> member.token,
      ReplicationAPIParams.Cookie -> cookie,
      ReplicationAPIParams.Mode -> mode.toString)
    startTimestamp.foreach(ts => subscribeRequest += ReplicationAPIParams.Start -> ts.value)

    val subscribeRequestMessage = new InMessage(subscribeRequest)
    var subscribeResponseMessage: Option[OutMessage] = None
    subscribeRequestMessage.replyCallback = (reply) => subscribeResponseMessage = Some(reply)

    sessionManager.handleSubscribeMessage(subscribeRequestMessage)
    val session = sessionManager.sessions.head
    session.member should be(ResolvedServiceMember(service, member))
    session.cookie should be(cookie)
    session.id should be(Some(sessionId))
    // Do not verify session mode, start and end timestamps. These values can be different from the ones requested
    // if the master fallback to a different mode.
    subscribeResponseMessage.get.error should be(None)

    session
  }

  /**
   * Generates a list of synthetic transaction records (Request + Response log records) finalized by an Index record.
   */
  def createTransactions(count: Int, initialTimestamp: Long = 0, timestampIncrement: Int = 1): List[LogRecord] = {

    def create(nextId: Long, timestamp: Long, consistentTimestamp: Option[Timestamp], remaining: Int): List[LogRecord] = {
      if (remaining > 0) {
        val message = createRequestMessage(timestamp)
        val request = Request(nextId, consistentTimestamp, message)
        trace("createTransactions: {}", request)
        val response = Response(request.id + 1, consistentTimestamp, createResponseMessage(message))
        trace("createTransactions: {}", response)
        request :: response :: create(response.id + 1, timestamp + timestampIncrement, Some(timestamp), remaining - 1)
      } else {
        val index = Index(nextId, consistentTimestamp)
        trace("createTransactions: {}", index)
        index :: Nil
      }
    }

    create(0, initialTimestamp, None, count)
  }

  /**
   * Convert the specified log records into transaction messages
   */
  def toTransactionMessages(records: Seq[LogRecord], startTimestamp: Long): Seq[Message] = {
    // Only keep Request records. Note that the start timestamp is exclusive.
    val requests = records.collect {
      case request: Request if request.timestamp > startTimestamp => request
    }

    requests.zipWithIndex.map {
      case (request, i) => {
        val publishParams: Map[String, MValue] = Map(
          ReplicationAPIParams.Timestamp -> request.timestamp.value,
          ReplicationAPIParams.SubscriptionId -> sessionId,
          ReplicationAPIParams.SessionId -> sessionId,
          ReplicationAPIParams.Sequence -> (i + 1).toLong)
        new OutMessage(publishParams, data = request.message)
      }
    }
  }

  def assertReplicationMessageEquals(actual: Message, expected: Message, ignoreSequence: Boolean = false) {
    if (ignoreSequence) {
      actual.parameters.toMap - ReplicationAPIParams.Sequence should be(expected.parameters.toMap - ReplicationAPIParams.Sequence)
    } else {
      actual.parameters.toMap should be(expected.parameters.toMap)
    }
    actual.getData[Message].parameters should be(expected.getData[Message].parameters)
  }

  def assertReplicationMessagesEquals(actual: Seq[Message], expected: Seq[Message], size: Int = -1, ignoreSequence: Boolean = false) {

    def getTimestamp(message: Message) = {
      if (message.hasData) message.getData[Message].timestamp else None
    }
    trace("assertPublishMessagesEquals: actual={}, expected={}", actual.map(getTimestamp(_)), expected.map(getTimestamp(_)))

    actual.zip(expected).foreach {
      pair => assertReplicationMessageEquals(pair._1, pair._2, ignoreSequence)
    }

    val expectedSize = if (size < 0) expected.size else size
    actual.size should be(expectedSize)
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

  test("subscribe should fail if not master of the service member") {
    val subscribeRequest: Map[String, MValue] = Map(
      ReplicationAPIParams.Token -> remoteMember.token.toString,
      ReplicationAPIParams.Cookie -> "cookie",
      ReplicationAPIParams.Start -> "12345",
      ReplicationAPIParams.Mode -> ReplicationMode.Store.toString)

    val subscribeRequestMessage = new InMessage(subscribeRequest)
    var subscribeResponseMessage: Option[OutMessage] = None
    subscribeRequestMessage.replyCallback = (reply) => subscribeResponseMessage = Some(reply)

    sessionManager.handleSubscribeMessage(subscribeRequestMessage)
    sessionManager.sessions should be(Nil) // synchronized actor message
    subscribeResponseMessage.get.error should not be None
    verifyZeroInteractions(mockPushAction)
  }

  test("subscribe should fail if service member does not exist") {
    val subscribeRequest: Map[String, MValue] = Map(
      ReplicationAPIParams.Token -> "1000",
      ReplicationAPIParams.Cookie -> "cookie",
      ReplicationAPIParams.Start -> "12345",
      ReplicationAPIParams.Mode -> ReplicationMode.Bootstrap.toString)

    val subscribeRequestMessage = new InMessage(subscribeRequest)
    var subscribeResponseMessage: Option[OutMessage] = None
    subscribeRequestMessage.replyCallback = (reply) => subscribeResponseMessage = Some(reply)

    sessionManager.handleSubscribeMessage(subscribeRequestMessage)
    sessionManager.sessions should be(Nil) // synchronized actor message
    subscribeResponseMessage.get.error should not be None
    verifyZeroInteractions(mockPushAction)
  }

  test("subscribe live mode should fail if master has no transaction log") {
    val subscribeRequest: Map[String, MValue] = Map(
      ReplicationAPIParams.Token -> member.token,
      ReplicationAPIParams.Cookie -> "cookie",
      ReplicationAPIParams.Start -> "12345",
      ReplicationAPIParams.Mode -> ReplicationMode.Live.toString)

    val subscribeRequestMessage = new InMessage(subscribeRequest)
    var subscribeResponseMessage: Option[OutMessage] = None
    subscribeRequestMessage.replyCallback = (reply) => subscribeResponseMessage = Some(reply)

    txLog.read.toList should be(Nil)

    sessionManager.handleSubscribeMessage(subscribeRequestMessage)
    sessionManager.sessions should be(Nil) // synchronized actor message
    subscribeResponseMessage.get.error should not be None
    verifyZeroInteractionsAfter(wait = 100, mockPushAction)
  }

  test("subscribe live mode should replicate expected transactions up to window size") {
    val logRecords = createTransactions(count = 10, initialTimestamp = 0)
    logRecords.foreach(txLog.append(_))
    logRecords should be(txLog.read.toList)
    currentConsistentTimestamp = logRecords.last.consistentTimestamp

    val startTimestamp = 1L
    val session = subscribe(Some(startTimestamp), ReplicationMode.Live)
    session.mode should be(ReplicationMode.Live)
    session.startTimestamp should be(Some(Timestamp(startTimestamp)))
    session.endTimestamp should be(None)

    val messageCaptor = ArgumentCaptor.forClass(classOf[OutMessage])
    verify(mockPushAction, timeout(1500).atLeast(replicationWindowSize)).callOutgoingHandlers(messageCaptor.capture())

    // Verify received expected messages up to the window size
    val expectedMessages = toTransactionMessages(logRecords, startTimestamp)
    val actualMessages = messageCaptor.getAllValues.toList
    expectedMessages.zip(actualMessages).foreach {
      case (expected, actual) => assertReplicationMessageEquals(actual = actual, expected = expected)
    }
    actualMessages.size should be(replicationWindowSize)
    verifyNoMoreInteractionsAfter(wait = 100, mockPushAction)
  }

  test("replying to first replication message after full window size should replicate a new transactions") {
    val logRecords = createTransactions(count = 10, initialTimestamp = 0)
    logRecords.foreach(txLog.append(_))
    logRecords should be(txLog.read.toList)
    currentConsistentTimestamp = logRecords.last.consistentTimestamp

    val startTimestamp = 1L
    val session = subscribe(Some(startTimestamp), ReplicationMode.Live)
    session.mode should be(ReplicationMode.Live)
    session.startTimestamp should be(Some(Timestamp(startTimestamp)))
    session.endTimestamp should be(None)

    val messageCaptor = ArgumentCaptor.forClass(classOf[OutMessage])
    verify(mockPushAction, timeout(1500).atLeast(replicationWindowSize)).callOutgoingHandlers(messageCaptor.capture())

    // Verify received expected messages up to the window size
    val expectedMessages = toTransactionMessages(logRecords, startTimestamp)
    val actualMessages = messageCaptor.getAllValues.toList
    expectedMessages.zip(actualMessages).foreach {
      case (expected, actual) => assertReplicationMessageEquals(actual = actual, expected = expected)
    }
    actualMessages.size should be(replicationWindowSize)
    verifyNoMoreInteractionsAfter(wait = 100, mockPushAction)

    // Verify a new message is replicated if first received message is replied
    reset(mockPushAction)
    actualMessages.head.handleReply(new InMessage())
    verify(mockPushAction, timeout(1500).times(1)).callOutgoingHandlers(messageCaptor.capture())
    assertReplicationMessageEquals(actual = messageCaptor.getValue, expected = expectedMessages(replicationWindowSize))
    verifyNoMoreInteractionsAfter(wait = 100, mockPushAction)
  }

  test("replying last replication message after full window size should NOT replicate a new transactions") {
    val logRecords = createTransactions(count = 10, initialTimestamp = 0)
    logRecords.foreach(txLog.append(_))
    logRecords should be(txLog.read.toList)
    currentConsistentTimestamp = logRecords.last.consistentTimestamp

    val startTimestamp = 1L
    val session = subscribe(Some(startTimestamp), ReplicationMode.Live)
    session.mode should be(ReplicationMode.Live)
    session.startTimestamp should be(Some(Timestamp(startTimestamp)))
    session.endTimestamp should be(None)

    val messageCaptor = ArgumentCaptor.forClass(classOf[OutMessage])
    verify(mockPushAction, timeout(1500).atLeast(replicationWindowSize)).callOutgoingHandlers(messageCaptor.capture())

    // Verify received expected messages up to the window size
    val expectedMessages = toTransactionMessages(logRecords, startTimestamp)
    val actualMessages = messageCaptor.getAllValues.toList
    expectedMessages.zip(actualMessages).foreach {
      case (expected, actual) => assertReplicationMessageEquals(actual = actual, expected = expected)
    }
    actualMessages.size should be(replicationWindowSize)
    verifyNoMoreInteractionsAfter(wait = 100, mockPushAction)

    // Verify NO new message is replicated if last received message is replied
    reset(mockPushAction)
    actualMessages.last.handleReply(new InMessage())
    verifyZeroInteractionsAfter(wait = 500, mockPushAction)
  }

  test("subscribe live mode should fallback to bootstrap mode if start timestamp is NOT specified") {
    val logRecords = createTransactions(count = 10, initialTimestamp = 0)
    logRecords.foreach(txLog.append(_))
    logRecords should be(txLog.read.toList)
    currentConsistentTimestamp = logRecords.last.consistentTimestamp

    val startTimestamp = Long.MinValue
    when(mockStore.readTransactions(startTimestamp, currentConsistentTimestamp.get, ranges)).thenReturn(
      toStoreIterator(logRecords, startTimestamp))

    // Subscribe without specifying a start timestamp
    val session = subscribe(startTimestamp = None, ReplicationMode.Live)
    session.mode should be(ReplicationMode.Bootstrap)
    session.startTimestamp should be(Some(Timestamp(startTimestamp)))
    session.endTimestamp should be(currentConsistentTimestamp)

    val messageCaptor = ArgumentCaptor.forClass(classOf[OutMessage])
    verify(mockPushAction, timeout(1500).atLeast(replicationWindowSize)).callOutgoingHandlers(messageCaptor.capture())

    // Verify received expected messages up to the window size
    val expectedMessages = toTransactionMessages(logRecords, Long.MinValue)
    val actualMessages = messageCaptor.getAllValues.toList
    expectedMessages.zip(actualMessages).foreach {
      case (expected, actual) => assertReplicationMessageEquals(actual = actual, expected = expected)
    }
    actualMessages.size should be(replicationWindowSize)
    verifyNoMoreInteractionsAfter(wait = 100, mockPushAction)
  }

  test("subscribe live mode should fallback to bootstrap mode if start timestamp is before first log timestamp") {
    val logRecords = createTransactions(count = 10, initialTimestamp = 0)
    // Append only second half of the transactions in log, first transactions will come from the store
    logRecords.slice(logRecords.size / 2, logRecords.size).foreach(txLog.append(_))
    currentConsistentTimestamp = logRecords.last.consistentTimestamp

    val startTimestamp = 1L
    when(mockStore.readTransactions(startTimestamp, currentConsistentTimestamp.get, ranges)).thenReturn(
      toStoreIterator(logRecords, startTimestamp))

    val session = subscribe(Some(startTimestamp), ReplicationMode.Live)
    session.mode should be(ReplicationMode.Bootstrap)
    session.startTimestamp should be(Some(Timestamp(startTimestamp)))
    session.endTimestamp should be(currentConsistentTimestamp)

    val messageCaptor = ArgumentCaptor.forClass(classOf[OutMessage])
    verify(mockPushAction, timeout(1500).atLeast(replicationWindowSize)).callOutgoingHandlers(messageCaptor.capture())

    // Verify received expected messages up to the window size
    val expectedMessages = toTransactionMessages(logRecords, startTimestamp)
    val actualMessages = messageCaptor.getAllValues.toList
    expectedMessages.zip(actualMessages).foreach {
      case (expected, actual) => assertReplicationMessageEquals(actual = actual, expected = expected)
    }
    actualMessages.size should be(replicationWindowSize)
    verifyNoMoreInteractionsAfter(wait = 100, mockPushAction)
  }

  test("subscribe bootstrap mode should replicated expected transactions up to window size") {
    val logRecords = createTransactions(count = 10, initialTimestamp = 0)
    // Append only second half of the transactions in log, first transactions will come from the store
    logRecords.slice(logRecords.size / 2, logRecords.size).foreach(txLog.append(_))
    currentConsistentTimestamp = logRecords.last.consistentTimestamp

    val startTimestamp = 1L
    when(mockStore.readTransactions(startTimestamp, currentConsistentTimestamp.get, ranges)).thenReturn(
      toStoreIterator(logRecords, startTimestamp))

    val session = subscribe(Some(startTimestamp), ReplicationMode.Bootstrap)
    session.mode should be(ReplicationMode.Bootstrap)
    session.startTimestamp should be(Some(Timestamp(startTimestamp)))
    session.endTimestamp should be(currentConsistentTimestamp)

    val messageCaptor = ArgumentCaptor.forClass(classOf[OutMessage])
    verify(mockPushAction, timeout(1000).atLeast(replicationWindowSize)).callOutgoingHandlers(messageCaptor.capture())

    // Verify received expected messages up to the window size
    val expectedMessages = toTransactionMessages(logRecords, startTimestamp)
    val actualMessages = messageCaptor.getAllValues.toList
    expectedMessages.zip(actualMessages).foreach {
      case (expected, actual) => assertReplicationMessageEquals(actual = actual, expected = expected)
    }
    actualMessages.size should be(replicationWindowSize)
    verifyNoMoreInteractionsAfter(wait = 100, mockPushAction)
  }

  test("subscribe bootstrap mode without log should fail") {
    val logRecords = createTransactions(count = 10, initialTimestamp = 0)
    currentConsistentTimestamp = logRecords.last.consistentTimestamp
    val startTimestamp = 1L
    when(mockStore.readTransactions(startTimestamp, currentConsistentTimestamp.get, ranges)).thenReturn(
      toStoreIterator(logRecords, startTimestamp))

    val subscribeRequest: Map[String, MValue] = Map(
      ReplicationAPIParams.Token -> member.token,
      ReplicationAPIParams.Cookie -> "cookie",
      ReplicationAPIParams.Start -> startTimestamp,
      ReplicationAPIParams.Mode -> ReplicationMode.Bootstrap.toString)

    val subscribeRequestMessage = new InMessage(subscribeRequest)
    var subscribeResponseMessage: Option[OutMessage] = None
    subscribeRequestMessage.replyCallback = (reply) => subscribeResponseMessage = Some(reply)

    sessionManager.handleSubscribeMessage(subscribeRequestMessage)
    sessionManager.sessions should be(Nil) // synchronized actor message
    subscribeResponseMessage.get.error should not be None
    verifyZeroInteractions(mockPushAction)
  }

  test("bootstrap mode should end session when reaching end timestamp") {
    val logRecords = createTransactions(count = 10, initialTimestamp = 0)
    logRecords.foreach(txLog.append(_))
    currentConsistentTimestamp = logRecords.last.consistentTimestamp
    replicationWindowSize = 100

    val startTimestamp = 1L
    when(mockStore.readTransactions(startTimestamp, currentConsistentTimestamp.get, ranges)).thenReturn(
      toStoreIterator(logRecords, startTimestamp))

    val session = subscribe(Some(startTimestamp), ReplicationMode.Bootstrap)
    session.mode should be(ReplicationMode.Bootstrap)
    session.startTimestamp should be(Some(Timestamp(startTimestamp)))
    session.endTimestamp should be(currentConsistentTimestamp)

    val expectedMessages = toTransactionMessages(logRecords, startTimestamp)

    val messageCaptor = ArgumentCaptor.forClass(classOf[OutMessage])
    verify(mockPushAction, timeout(1500).atLeast(expectedMessages.size)).callOutgoingHandlers(messageCaptor.capture())

    // Verify received all expected messages
    val actualMessages = messageCaptor.getAllValues
    assertReplicationMessagesEquals(actualMessages, expectedMessages)
    verifyNoMoreInteractionsAfter(wait = 100, mockPushAction)
    sessionManager.sessions should be(Nil) // Session should be terminated after reaching end timestamp
  }

  test("live mode should end session when reaching end of log file (drain mode)") {
    val logRecords = createTransactions(count = 10, initialTimestamp = 0)
    logRecords.foreach(txLog.append(_))
    currentConsistentTimestamp = logRecords.last.consistentTimestamp
    replicationWindowSize = 100

    val startTimestamp = 1L

    val session = subscribe(Some(startTimestamp), ReplicationMode.Live)
    session.mode should be(ReplicationMode.Live)
    session.startTimestamp should be(Some(Timestamp(startTimestamp)))
    session.endTimestamp should be(None)

    val expectedMessages = toTransactionMessages(logRecords, startTimestamp)

    val messageCaptor = ArgumentCaptor.forClass(classOf[OutMessage])
    member.setStatus(MemberStatus.Leaving, triggerEvent = false)
    verify(mockPushAction, timeout(1500).atLeast(expectedMessages.size)).callOutgoingHandlers(messageCaptor.capture())

    // Verify received all expected messages
    val actualMessages = messageCaptor.getAllValues
    assertReplicationMessagesEquals(actualMessages, expectedMessages)
    verifyNoMoreInteractionsAfter(wait = 100, mockPushAction)
    sessionManager.sessions should be(Nil) // Session should be terminated after reaching end timestamp
  }

  test("live mode should send idle message when reaching consistent timestamp and resume replicating transaction when it increase") {
    val logRecords = createTransactions(count = 10, initialTimestamp = 0)
    logRecords.foreach(txLog.append(_))
    currentConsistentTimestamp = Some(7L)
    replicationWindowSize = 100

    val startTimestamp = 1L

    val session = subscribe(Some(startTimestamp), ReplicationMode.Live)
    session.mode should be(ReplicationMode.Live)
    session.startTimestamp should be(Some(Timestamp(startTimestamp)))
    session.endTimestamp should be(None)

    val expectedMessages = toTransactionMessages(logRecords, startTimestamp)

    val messageCaptor = ArgumentCaptor.forClass(classOf[OutMessage])
    verify(mockPushAction, timeout(1500).atLeast(5)).callOutgoingHandlers(messageCaptor.capture())

    // Verify received all expected messages until to consistent timestamp (i.e. 2, 3, 4, 5, 6)
    val actualMessages = messageCaptor.getAllValues.toList
    assertReplicationMessagesEquals(actualMessages, expectedMessages, size = 5)
    actualMessages.size should be(5)

    // Wait for idle message
    reset(mockPushAction)
    verify(mockPushAction, timeout(1500).times(1)).callOutgoingHandlers(messageCaptor.capture())
    messageCaptor.getValue.hasData should be(false)

    // Advance consistent timestamp and verify a new transaction is replicated
    reset(mockPushAction)
    currentConsistentTimestamp = Some(8L)
    verify(mockPushAction, timeout(1500).times(1)).callOutgoingHandlers(messageCaptor.capture())
    messageCaptor.getValue.getData[Message].timestamp should be(Some(Timestamp(7L)))
    verifyNoMoreInteractionsAfter(wait = 100, mockPushAction)
  }

  test("live mode should replicate new transactions when appended in log and consistent timestamp increase") {
    val allLogRecords = createTransactions(count = 20, initialTimestamp = 0)
    val (logRecords, newLogRecords) = allLogRecords.splitAt(allLogRecords.size / 2)
    logRecords.foreach(txLog.append(_))
    currentConsistentTimestamp = logRecords.last.consistentTimestamp
    replicationWindowSize = 100

    val startTimestamp = 1L

    val session = subscribe(Some(startTimestamp), ReplicationMode.Live)
    session.mode should be(ReplicationMode.Live)
    session.startTimestamp should be(Some(Timestamp(startTimestamp)))
    session.endTimestamp should be(None)

    val allExpectedMessages = toTransactionMessages(allLogRecords, startTimestamp)
    val (expectedMessages, newExpectedMessages) = allExpectedMessages.partition(
      _.getData[Message].timestamp.get < logRecords.last.consistentTimestamp.get.value)

    val messageCaptor = ArgumentCaptor.forClass(classOf[OutMessage])
    verify(mockPushAction, timeout(1500).atLeast(expectedMessages.size)).callOutgoingHandlers(messageCaptor.capture())

    // Verify received all expected messages
    val actualMessages = messageCaptor.getAllValues
    assertReplicationMessagesEquals(actualMessages, expectedMessages)
    actualMessages.size should be > 0

    // Append new log records and verify they are replicated after advancing the consistent timestamp
    reset(mockPushAction)
    newLogRecords.foreach(txLog.append(_))
    currentConsistentTimestamp = newLogRecords.last.consistentTimestamp

    val newMessageCaptor = ArgumentCaptor.forClass(classOf[OutMessage])
    verify(mockPushAction, timeout(1500).atLeast(newExpectedMessages.size)).callOutgoingHandlers(newMessageCaptor.capture())
    assertReplicationMessagesEquals(newMessageCaptor.getAllValues.filter(_.hasData), newExpectedMessages,
      size = newExpectedMessages.size - 1, ignoreSequence = true)

    verifyNoMoreInteractionsAfter(wait = 100, mockPushAction)
  }

  test("unsubscribe should kill session") {
    val logRecords = createTransactions(count = 10, initialTimestamp = 0)
    logRecords.foreach(txLog.append(_))
    currentConsistentTimestamp = logRecords.last.consistentTimestamp

    val startTimestamp = 1L
    val session = subscribe(Some(startTimestamp), ReplicationMode.Live)
    session.mode should be(ReplicationMode.Live)
    session.startTimestamp should be(Some(Timestamp(startTimestamp)))
    session.endTimestamp should be(None)
    sessionManager.sessions should be(List(session))

    val unsubscribeRequest: Map[String, MValue] = Map(
      ReplicationAPIParams.SessionId -> session.id.get)
    val unsubscribeRequestMessage = new InMessage(unsubscribeRequest)
    var unsubscribeResponseMessage: Option[OutMessage] = None
    unsubscribeRequestMessage.replyCallback = (reply) => unsubscribeResponseMessage = Some(reply)

    sessionManager.handleUnsubscribeMessage(unsubscribeRequestMessage)
    sessionManager.sessions should be(Nil)
    unsubscribeResponseMessage.get.error should be(None)
    verifyZeroInteractionsAfter(wait = 100, mockPushAction)
  }

  test("unsubscribe unknown session should do nothing") {
    val logRecords = createTransactions(count = 10, initialTimestamp = 0)
    logRecords.foreach(txLog.append(_))
    currentConsistentTimestamp = logRecords.last.consistentTimestamp

    val startTimestamp = 1L
    val session = subscribe(Some(startTimestamp), ReplicationMode.Live)
    session.mode should be(ReplicationMode.Live)
    session.startTimestamp should be(Some(Timestamp(startTimestamp)))
    session.endTimestamp should be(None)
    sessionManager.sessions should be(List(session))

    val unsubscribeRequest: Map[String, MValue] = Map(
      ReplicationAPIParams.SessionId -> "bad id")
    val unsubscribeRequestMessage = new InMessage(unsubscribeRequest)
    var unsubscribeResponseMessage: Option[OutMessage] = None
    unsubscribeRequestMessage.replyCallback = (reply) => unsubscribeResponseMessage = Some(reply)

    sessionManager.handleUnsubscribeMessage(unsubscribeRequestMessage)
    sessionManager.sessions should be(List(session))
    unsubscribeResponseMessage.get.error should be(None)
  }

  test("terminate member sessions should skill all member sessions") {
    val logRecords = createTransactions(count = 10, initialTimestamp = 0)
    logRecords.foreach(txLog.append(_))
    currentConsistentTimestamp = logRecords.last.consistentTimestamp

    val startTimestamp = 1L
    val session1 = subscribe(Some(startTimestamp), ReplicationMode.Live)
    val session2 = subscribe(Some(startTimestamp), ReplicationMode.Live)
    sessionManager.sessions should be(List(session1, session2))

    sessionManager.terminateMemberSessions(ResolvedServiceMember(service, member))
    sessionManager.sessions should be(Nil)
  }
}
