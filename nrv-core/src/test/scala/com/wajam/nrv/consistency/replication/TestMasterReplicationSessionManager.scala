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
import com.wajam.nrv.consistency.persistence.LogRecord.{Response, Request}
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
  var mockSlaveReplicateTxAction: Action = null
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

    logDir = Files.createTempDirectory("TestMasterReplicationSessionManager").toFile
    txLog = new FileTransactionLog(service.name, member.token, logDir = logDir.getAbsolutePath)

    mockStore = mock[ConsistentStore]
    mockSlaveReplicateTxAction = mock[Action]

    sessionManager = new MasterReplicationSessionManager(service, mockStore, (_) => txLog, (_) => currentConsistentTimestamp,
      ActionProxy(mockSlaveReplicateTxAction), pushTps = 100, replicationWindowSize, sessionTimeout) {
      override def nextId = sessionId
    }
    sessionManager.start()
  }

  after {
    sessionManager.stop()
    sessionManager = null
    mockSlaveReplicateTxAction = null
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
   * Utility to create a master replication session
   */
  def openSession(startTimestamp: Option[Timestamp], mode: ReplicationMode): ReplicationSession = {
    val cookie = "cookie"

    var openSessionRequest: Map[String, MValue] = Map(
      ReplicationAPIParams.Token -> member.token,
      ReplicationAPIParams.Cookie -> cookie,
      ReplicationAPIParams.Mode -> mode.toString)
    startTimestamp.foreach(ts => openSessionRequest += ReplicationAPIParams.Start -> ts.value)

    val openSessionRequestMessage = new InMessage(openSessionRequest)
    var openSessionResponseMessage: Option[OutMessage] = None
    openSessionRequestMessage.replyCallback = (reply) => openSessionResponseMessage = Some(reply)

    sessionManager.handleOpenSessionMessage(openSessionRequestMessage)
    val session = sessionManager.sessions.head
    session.member should be(ResolvedServiceMember(service, member))
    session.cookie should be(cookie)
    session.id should be(Some(sessionId))
    // Do not verify session mode, start and end timestamps. These values can be different from the ones requested
    // if the master fallback to a different mode.
    openSessionResponseMessage.get.error should be(None)

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
    trace("assertReplicationMessagesEquals: actual={}, expected={}", actual.map(getTimestamp(_)), expected.map(getTimestamp(_)))

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

  test("open session should fail if not master of the service member") {
    val openSessionRequest: Map[String, MValue] = Map(
      ReplicationAPIParams.Token -> remoteMember.token.toString,
      ReplicationAPIParams.Cookie -> "cookie",
      ReplicationAPIParams.Start -> "12345",
      ReplicationAPIParams.Mode -> ReplicationMode.Bootstrap.toString)

    val openSessionRequestMessage = new InMessage(openSessionRequest)
    var openSessionResponseMessage: Option[OutMessage] = None
    openSessionRequestMessage.replyCallback = (reply) => openSessionResponseMessage = Some(reply)

    sessionManager.handleOpenSessionMessage(openSessionRequestMessage)
    sessionManager.sessions should be(Nil) // synchronized actor message
    openSessionResponseMessage.get.error should not be None
    verifyZeroInteractions(mockSlaveReplicateTxAction)
  }

  test("open session should fail if service member does not exist") {
    val openSessionRequest: Map[String, MValue] = Map(
      ReplicationAPIParams.Token -> "1000",
      ReplicationAPIParams.Cookie -> "cookie",
      ReplicationAPIParams.Start -> "12345",
      ReplicationAPIParams.Mode -> ReplicationMode.Bootstrap.toString)

    val openSessionRequestMessage = new InMessage(openSessionRequest)
    var openSessionResponseMessage: Option[OutMessage] = None
    openSessionRequestMessage.replyCallback = (reply) => openSessionResponseMessage = Some(reply)

    sessionManager.handleOpenSessionMessage(openSessionRequestMessage)
    sessionManager.sessions should be(Nil) // synchronized actor message
    openSessionResponseMessage.get.error should not be None
    verifyZeroInteractions(mockSlaveReplicateTxAction)
  }

  test("open session live mode should fail if master has no transaction log") {
    val openSessionRequest: Map[String, MValue] = Map(
      ReplicationAPIParams.Token -> member.token,
      ReplicationAPIParams.Cookie -> "cookie",
      ReplicationAPIParams.Start -> "12345",
      ReplicationAPIParams.Mode -> ReplicationMode.Live.toString)

    val openSessionRequestMessage = new InMessage(openSessionRequest)
    var openSessionResponseMessage: Option[OutMessage] = None
    openSessionRequestMessage.replyCallback = (reply) => openSessionResponseMessage = Some(reply)

    txLog.read.toList should be(Nil)

    sessionManager.handleOpenSessionMessage(openSessionRequestMessage)
    sessionManager.sessions should be(Nil) // synchronized actor message
    openSessionResponseMessage.get.error should not be None
    verifyZeroInteractionsAfter(wait = 100, mockSlaveReplicateTxAction)
  }

  test("open session live mode should replicate expected transactions up to window size") {
    val logRecords = createTransactions(count = 10, initialTimestamp = 0)
    logRecords.foreach(txLog.append(_))
    logRecords should be(txLog.read.toList)
    currentConsistentTimestamp = logRecords.last.consistentTimestamp

    val startTimestamp = 1L
    val session = openSession(Some(startTimestamp), ReplicationMode.Live)
    session.mode should be(ReplicationMode.Live)
    session.startTimestamp should be(Some(Timestamp(startTimestamp)))
    session.endTimestamp should be(None)

    val messageCaptor = ArgumentCaptor.forClass(classOf[OutMessage])
    verify(mockSlaveReplicateTxAction, timeout(1500).atLeast(replicationWindowSize)).callOutgoingHandlers(messageCaptor.capture())

    // Verify received expected messages up to the window size
    val expectedTxMessages = toTransactionMessages(logRecords, startTimestamp)
    val actualTxMessages = messageCaptor.getAllValues.toList
    expectedTxMessages.zip(actualTxMessages).foreach {
      case (expected, actual) => assertReplicationMessageEquals(actual = actual, expected = expected)
    }
    actualTxMessages.size should be(replicationWindowSize)
    verifyNoMoreInteractionsAfter(wait = 100, mockSlaveReplicateTxAction)
  }

  test("replying to first replication message after full window size should replicate a new transactions") {
    val logRecords = createTransactions(count = 10, initialTimestamp = 0)
    logRecords.foreach(txLog.append(_))
    logRecords should be(txLog.read.toList)
    currentConsistentTimestamp = logRecords.last.consistentTimestamp

    val startTimestamp = 1L
    val session = openSession(Some(startTimestamp), ReplicationMode.Live)
    session.mode should be(ReplicationMode.Live)
    session.startTimestamp should be(Some(Timestamp(startTimestamp)))
    session.endTimestamp should be(None)

    val messageCaptor = ArgumentCaptor.forClass(classOf[OutMessage])
    verify(mockSlaveReplicateTxAction, timeout(1500).atLeast(replicationWindowSize)).callOutgoingHandlers(messageCaptor.capture())

    // Verify received expected messages up to the window size
    val expectedTxMessages = toTransactionMessages(logRecords, startTimestamp)
    val actualTxMessages = messageCaptor.getAllValues.toList
    expectedTxMessages.zip(actualTxMessages).foreach {
      case (expected, actual) => assertReplicationMessageEquals(actual = actual, expected = expected)
    }
    actualTxMessages.size should be(replicationWindowSize)
    verifyNoMoreInteractionsAfter(wait = 100, mockSlaveReplicateTxAction)

    // Verify a new message is replicated if first received message is replied
    reset(mockSlaveReplicateTxAction)
    actualTxMessages.head.handleReply(new InMessage())
    verify(mockSlaveReplicateTxAction, timeout(1500).times(1)).callOutgoingHandlers(messageCaptor.capture())
    assertReplicationMessageEquals(actual = messageCaptor.getValue, expected = expectedTxMessages(replicationWindowSize))
    verifyNoMoreInteractionsAfter(wait = 100, mockSlaveReplicateTxAction)
  }

  test("replying last replication message after full window size should NOT replicate a new transactions") {
    val logRecords = createTransactions(count = 10, initialTimestamp = 0)
    logRecords.foreach(txLog.append(_))
    logRecords should be(txLog.read.toList)
    currentConsistentTimestamp = logRecords.last.consistentTimestamp

    val startTimestamp = 1L
    val session = openSession(Some(startTimestamp), ReplicationMode.Live)
    session.mode should be(ReplicationMode.Live)
    session.startTimestamp should be(Some(Timestamp(startTimestamp)))
    session.endTimestamp should be(None)

    val messageCaptor = ArgumentCaptor.forClass(classOf[OutMessage])
    verify(mockSlaveReplicateTxAction, timeout(1500).atLeast(replicationWindowSize)).callOutgoingHandlers(messageCaptor.capture())

    // Verify received expected messages up to the window size
    val expectedTxMessages = toTransactionMessages(logRecords, startTimestamp)
    val actualTxMessages = messageCaptor.getAllValues.toList
    expectedTxMessages.zip(actualTxMessages).foreach {
      case (expected, actual) => assertReplicationMessageEquals(actual = actual, expected = expected)
    }
    actualTxMessages.size should be(replicationWindowSize)
    verifyNoMoreInteractionsAfter(wait = 100, mockSlaveReplicateTxAction)

    // Verify NO new message is replicated if last received message is replied
    reset(mockSlaveReplicateTxAction)
    actualTxMessages.last.handleReply(new InMessage())
    verifyZeroInteractionsAfter(wait = 500, mockSlaveReplicateTxAction)
  }

  test("open session live mode should fallback to bootstrap mode if start timestamp is NOT specified") {
    val logRecords = createTransactions(count = 10, initialTimestamp = 0)
    logRecords.foreach(txLog.append(_))
    logRecords should be(txLog.read.toList)
    currentConsistentTimestamp = logRecords.last.consistentTimestamp

    val startTimestamp = Long.MinValue
    when(mockStore.readTransactions(startTimestamp, currentConsistentTimestamp.get, ranges)).thenReturn(
      toStoreIterator(logRecords, startTimestamp))

    // Open session without specifying a start timestamp
    val session = openSession(startTimestamp = None, ReplicationMode.Live)
    session.mode should be(ReplicationMode.Bootstrap)
    session.startTimestamp should be(Some(Timestamp(startTimestamp)))
    session.endTimestamp should be(currentConsistentTimestamp)

    val messageCaptor = ArgumentCaptor.forClass(classOf[OutMessage])
    verify(mockSlaveReplicateTxAction, timeout(1500).atLeast(replicationWindowSize)).callOutgoingHandlers(messageCaptor.capture())

    // Verify received expected messages up to the window size
    val expectedTxMessages = toTransactionMessages(logRecords, Long.MinValue)
    val actualTxMessages = messageCaptor.getAllValues.toList
    expectedTxMessages.zip(actualTxMessages).foreach {
      case (expected, actual) => assertReplicationMessageEquals(actual = actual, expected = expected)
    }
    actualTxMessages.size should be(replicationWindowSize)
    verifyNoMoreInteractionsAfter(wait = 100, mockSlaveReplicateTxAction)
  }

  test("open session live mode should fallback to bootstrap mode if start timestamp is before first log timestamp") {
    val logRecords = createTransactions(count = 10, initialTimestamp = 0)
    // Append only second half of the transactions in log, first transactions will come from the store
    logRecords.slice(logRecords.size / 2, logRecords.size).foreach(txLog.append(_))
    currentConsistentTimestamp = logRecords.last.consistentTimestamp

    val startTimestamp = 1L
    when(mockStore.readTransactions(startTimestamp, currentConsistentTimestamp.get, ranges)).thenReturn(
      toStoreIterator(logRecords, startTimestamp))

    val session = openSession(Some(startTimestamp), ReplicationMode.Live)
    session.mode should be(ReplicationMode.Bootstrap)
    session.startTimestamp should be(Some(Timestamp(startTimestamp)))
    session.endTimestamp should be(currentConsistentTimestamp)

    val messageCaptor = ArgumentCaptor.forClass(classOf[OutMessage])
    verify(mockSlaveReplicateTxAction, timeout(1500).atLeast(replicationWindowSize)).callOutgoingHandlers(messageCaptor.capture())

    // Verify received expected messages up to the window size
    val expectedTxMessages = toTransactionMessages(logRecords, startTimestamp)
    val actualTxMessages = messageCaptor.getAllValues.toList
    expectedTxMessages.zip(actualTxMessages).foreach {
      case (expected, actual) => assertReplicationMessageEquals(actual = actual, expected = expected)
    }
    actualTxMessages.size should be(replicationWindowSize)
    verifyNoMoreInteractionsAfter(wait = 100, mockSlaveReplicateTxAction)
  }

  test("open session bootstrap mode should replicated expected transactions up to window size") {
    val logRecords = createTransactions(count = 10, initialTimestamp = 0)
    // Append only second half of the transactions in log, first transactions will come from the store
    logRecords.slice(logRecords.size / 2, logRecords.size).foreach(txLog.append(_))
    currentConsistentTimestamp = logRecords.last.consistentTimestamp

    val startTimestamp = 1L
    when(mockStore.readTransactions(startTimestamp, currentConsistentTimestamp.get, ranges)).thenReturn(
      toStoreIterator(logRecords, startTimestamp))

    val session = openSession(Some(startTimestamp), ReplicationMode.Bootstrap)
    session.mode should be(ReplicationMode.Bootstrap)
    session.startTimestamp should be(Some(Timestamp(startTimestamp)))
    session.endTimestamp should be(currentConsistentTimestamp)

    val messageCaptor = ArgumentCaptor.forClass(classOf[OutMessage])
    verify(mockSlaveReplicateTxAction, timeout(1000).atLeast(replicationWindowSize)).callOutgoingHandlers(messageCaptor.capture())

    // Verify received expected messages up to the window size
    val expectedTxMessages = toTransactionMessages(logRecords, startTimestamp)
    val actualTxMessages = messageCaptor.getAllValues.toList
    expectedTxMessages.zip(actualTxMessages).foreach {
      case (expected, actual) => assertReplicationMessageEquals(actual = actual, expected = expected)
    }
    actualTxMessages.size should be(replicationWindowSize)
    verifyNoMoreInteractionsAfter(wait = 100, mockSlaveReplicateTxAction)
  }

  test("open session bootstrap mode without log should fail") {
    val logRecords = createTransactions(count = 10, initialTimestamp = 0)
    currentConsistentTimestamp = logRecords.last.consistentTimestamp
    val startTimestamp = 1L
    when(mockStore.readTransactions(startTimestamp, currentConsistentTimestamp.get, ranges)).thenReturn(
      toStoreIterator(logRecords, startTimestamp))

    val openSessionRequest: Map[String, MValue] = Map(
      ReplicationAPIParams.Token -> member.token,
      ReplicationAPIParams.Cookie -> "cookie",
      ReplicationAPIParams.Start -> startTimestamp,
      ReplicationAPIParams.Mode -> ReplicationMode.Bootstrap.toString)

    val openSessionRequestMessage = new InMessage(openSessionRequest)
    var openSessionResponseMessage: Option[OutMessage] = None
    openSessionRequestMessage.replyCallback = (reply) => openSessionResponseMessage = Some(reply)

    sessionManager.handleOpenSessionMessage(openSessionRequestMessage)
    sessionManager.sessions should be(Nil) // synchronized actor message
    openSessionResponseMessage.get.error should not be None
    verifyZeroInteractions(mockSlaveReplicateTxAction)
  }

  test("bootstrap mode should end session when reaching end timestamp") {
    val logRecords = createTransactions(count = 10, initialTimestamp = 0)
    logRecords.foreach(txLog.append(_))
    currentConsistentTimestamp = logRecords.last.consistentTimestamp
    replicationWindowSize = 100

    val startTimestamp = 1L
    when(mockStore.readTransactions(startTimestamp, currentConsistentTimestamp.get, ranges)).thenReturn(
      toStoreIterator(logRecords, startTimestamp))

    val session = openSession(Some(startTimestamp), ReplicationMode.Bootstrap)
    session.mode should be(ReplicationMode.Bootstrap)
    session.startTimestamp should be(Some(Timestamp(startTimestamp)))
    session.endTimestamp should be(currentConsistentTimestamp)

    val expectedTxMessages = toTransactionMessages(logRecords, startTimestamp)

    val messageCaptor = ArgumentCaptor.forClass(classOf[OutMessage])
    verify(mockSlaveReplicateTxAction, timeout(1500).atLeast(expectedTxMessages.size)).callOutgoingHandlers(messageCaptor.capture())

    // Verify received all expected messages
    val actualTxMessages = messageCaptor.getAllValues
    assertReplicationMessagesEquals(actualTxMessages, expectedTxMessages)
    verifyNoMoreInteractionsAfter(wait = 100, mockSlaveReplicateTxAction)
    sessionManager.sessions should be(Nil) // Session should be terminated after reaching end timestamp
  }

  test("live mode should end session when reaching end of log file (drain mode)") {
    val logRecords = createTransactions(count = 10, initialTimestamp = 0)
    logRecords.foreach(txLog.append(_))
    currentConsistentTimestamp = logRecords.last.consistentTimestamp
    replicationWindowSize = 100

    val startTimestamp = 1L

    val session = openSession(Some(startTimestamp), ReplicationMode.Live)
    session.mode should be(ReplicationMode.Live)
    session.startTimestamp should be(Some(Timestamp(startTimestamp)))
    session.endTimestamp should be(None)

    val expectedTxMessages = toTransactionMessages(logRecords, startTimestamp)

    val messageCaptor = ArgumentCaptor.forClass(classOf[OutMessage])
    member.setStatus(MemberStatus.Leaving, triggerEvent = false)
    verify(mockSlaveReplicateTxAction, timeout(1500).atLeast(expectedTxMessages.size)).callOutgoingHandlers(messageCaptor.capture())

    // Verify received all expected messages
    val actualTxMessages = messageCaptor.getAllValues
    assertReplicationMessagesEquals(actualTxMessages, expectedTxMessages)
    verifyNoMoreInteractionsAfter(wait = 100, mockSlaveReplicateTxAction)
    sessionManager.sessions should be(Nil) // Session should be terminated after reaching end timestamp
  }

  test("live mode should send idle message when reaching consistent timestamp and resume replicating transaction when it increase") {
    val logRecords = createTransactions(count = 10, initialTimestamp = 0)
    logRecords.foreach(txLog.append(_))
    currentConsistentTimestamp = Some(7L)
    replicationWindowSize = 100

    val startTimestamp = 1L

    val session = openSession(Some(startTimestamp), ReplicationMode.Live)
    session.mode should be(ReplicationMode.Live)
    session.startTimestamp should be(Some(Timestamp(startTimestamp)))
    session.endTimestamp should be(None)

    val expectedTxMessages = toTransactionMessages(logRecords, startTimestamp)

    val messageCaptor = ArgumentCaptor.forClass(classOf[OutMessage])
    verify(mockSlaveReplicateTxAction, timeout(1500).atLeast(6)).callOutgoingHandlers(messageCaptor.capture())

    // Verify received all expected messages until to consistent timestamp (i.e. 2, 3, 4, 5, 6, 7)
    val actualTxMessages = messageCaptor.getAllValues.toList
    assertReplicationMessagesEquals(actualTxMessages, expectedTxMessages, size = 6)
    actualTxMessages.size should be(6)

    // Wait for idle message
    reset(mockSlaveReplicateTxAction)
    verify(mockSlaveReplicateTxAction, timeout(1500).times(1)).callOutgoingHandlers(messageCaptor.capture())
    messageCaptor.getValue.hasData should be(false)

    // Advance consistent timestamp and verify a new transaction is replicated
    reset(mockSlaveReplicateTxAction)
    currentConsistentTimestamp = Some(8L)
    verify(mockSlaveReplicateTxAction, timeout(1500).times(1)).callOutgoingHandlers(messageCaptor.capture())
    messageCaptor.getValue.getData[Message].timestamp should be(Some(Timestamp(8L)))
    verifyNoMoreInteractionsAfter(wait = 100, mockSlaveReplicateTxAction)
  }

  test("live mode should replicate new transactions when appended in log and consistent timestamp increase") {
    val allLogRecords = createTransactions(count = 20, initialTimestamp = 0)
    val (logRecords, newLogRecords) = allLogRecords.splitAt(allLogRecords.size / 2)
    logRecords.foreach(txLog.append(_))
    currentConsistentTimestamp = logRecords.last.consistentTimestamp
    replicationWindowSize = 100

    val startTimestamp = 1L

    val session = openSession(Some(startTimestamp), ReplicationMode.Live)
    session.mode should be(ReplicationMode.Live)
    session.startTimestamp should be(Some(Timestamp(startTimestamp)))
    session.endTimestamp should be(None)

    val allexpectedTxMessages = toTransactionMessages(allLogRecords, startTimestamp)
    val (expectedTxMessages, newExpectedTxMessages) = allexpectedTxMessages.partition(
      _.getData[Message].timestamp.get <= logRecords.last.consistentTimestamp.get.value)

    val messageCaptor = ArgumentCaptor.forClass(classOf[OutMessage])
    verify(mockSlaveReplicateTxAction, timeout(1500).atLeast(expectedTxMessages.size)).callOutgoingHandlers(messageCaptor.capture())

    // Verify received all expected messages
    val actualTxMessages = messageCaptor.getAllValues
    assertReplicationMessagesEquals(actualTxMessages, expectedTxMessages)
    actualTxMessages.size should be > 0

    // Append new log records and verify they are replicated after advancing the consistent timestamp
    reset(mockSlaveReplicateTxAction)
    newLogRecords.foreach(txLog.append(_))
    currentConsistentTimestamp = newLogRecords.last.consistentTimestamp

    val newMessageCaptor = ArgumentCaptor.forClass(classOf[OutMessage])
    verify(mockSlaveReplicateTxAction, timeout(1500).atLeast(newExpectedTxMessages.size)).callOutgoingHandlers(newMessageCaptor.capture())
    assertReplicationMessagesEquals(newMessageCaptor.getAllValues.filter(_.hasData), newExpectedTxMessages,
      size = newExpectedTxMessages.size, ignoreSequence = true)

    verifyNoMoreInteractionsAfter(wait = 100, mockSlaveReplicateTxAction)
  }

  test("close session should kill session") {
    val logRecords = createTransactions(count = 10, initialTimestamp = 0)
    logRecords.foreach(txLog.append(_))
    currentConsistentTimestamp = logRecords.last.consistentTimestamp

    val startTimestamp = 1L
    val session = openSession(Some(startTimestamp), ReplicationMode.Live)
    session.mode should be(ReplicationMode.Live)
    session.startTimestamp should be(Some(Timestamp(startTimestamp)))
    session.endTimestamp should be(None)
    sessionManager.sessions should be(List(session))

    val closeSessionRequest: Map[String, MValue] = Map(
      ReplicationAPIParams.SessionId -> session.id.get)
    val closeSessionRequestMessage = new InMessage(closeSessionRequest)
    var closeSessionResponseMessage: Option[OutMessage] = None
    closeSessionRequestMessage.replyCallback = (reply) => closeSessionResponseMessage = Some(reply)

    sessionManager.handleCloseSessionMessage(closeSessionRequestMessage)
    sessionManager.sessions should be(Nil)
    closeSessionResponseMessage.get.error should be(None)
    verifyZeroInteractionsAfter(wait = 100, mockSlaveReplicateTxAction)
  }

  test("close session unknown session should do nothing") {
    val logRecords = createTransactions(count = 10, initialTimestamp = 0)
    logRecords.foreach(txLog.append(_))
    currentConsistentTimestamp = logRecords.last.consistentTimestamp

    val startTimestamp = 1L
    val session = openSession(Some(startTimestamp), ReplicationMode.Live)
    session.mode should be(ReplicationMode.Live)
    session.startTimestamp should be(Some(Timestamp(startTimestamp)))
    session.endTimestamp should be(None)
    sessionManager.sessions should be(List(session))

    val closeSessionRequest: Map[String, MValue] = Map(
      ReplicationAPIParams.SessionId -> "bad id")
    val closeSessionRequestMessage = new InMessage(closeSessionRequest)
    var closeSessionResponseMessage: Option[OutMessage] = None
    closeSessionRequestMessage.replyCallback = (reply) => closeSessionResponseMessage = Some(reply)

    sessionManager.handleCloseSessionMessage(closeSessionRequestMessage)
    sessionManager.sessions should be(List(session))
    closeSessionResponseMessage.get.error should be(None)
  }

  test("terminate member sessions should skill all member sessions") {
    val logRecords = createTransactions(count = 10, initialTimestamp = 0)
    logRecords.foreach(txLog.append(_))
    currentConsistentTimestamp = logRecords.last.consistentTimestamp

    val startTimestamp = 1L
    val session1 = openSession(Some(startTimestamp), ReplicationMode.Live)
    val session2 = openSession(Some(startTimestamp), ReplicationMode.Live)
    sessionManager.sessions should be(List(session1, session2))

    sessionManager.terminateMemberSessions(ResolvedServiceMember(service, member))
    sessionManager.sessions should be(Nil)
  }
}
