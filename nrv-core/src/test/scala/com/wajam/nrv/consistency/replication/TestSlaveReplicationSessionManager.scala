package com.wajam.nrv.consistency.replication

import org.scalatest.BeforeAndAfter
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._
import org.scalatest.matchers.ShouldMatchers._
import org.mockito.Matchers._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import com.wajam.nrv.service._
import com.wajam.nrv.service.ActionProxy._
import com.wajam.nrv.consistency.{TestTransactionBase, ResolvedServiceMember, ConsistentStore}
import com.wajam.nrv.data._
import MessageMatcher._
import com.wajam.nrv.consistency.log.LogRecord.Index
import java.util.UUID
import com.wajam.nrv.consistency.log.{TransactionLogProxy, LogRecord}
import com.wajam.nrv.cluster.{Node, LocalNode, StaticClusterManager, Cluster}
import com.wajam.commons.IdGenerator
import com.wajam.nrv.utils.timestamp.Timestamp

@RunWith(classOf[JUnitRunner])
class TestSlaveReplicationSessionManager extends TestTransactionBase with BeforeAndAfter with MockitoSugar {

  var localNode: LocalNode = null
  var cluster: Cluster = null
  var service: Service = null
  var member: ResolvedServiceMember = null
  var txLogProxy: TransactionLogProxy = null
  var mockStore: ConsistentStore = null
  var mockMasterOpenSessionAction: Action = null
  var mockMasterCloseSessionAction: Action = null

  var currentCookie: String = null
  var currentLogId: Long = 0
  var sessionManager: SlaveReplicationSessionManager = null

  val sessionTimeout = 1500L
  val token = 0
  var sessionEndErrors: List[Exception] = Nil
  var sessionEndCalls = 0
  var pushTxReplyCount = 0

  before {
    localNode = new LocalNode(Map("nrv" -> 9999))
    cluster = new Cluster(localNode, new StaticClusterManager)
    service = new Service("service")
    service.applySupport(cluster = Some(cluster))
    member = ResolvedServiceMember(service.name, token, Seq(TokenRange.All))
    txLogProxy = new TransactionLogProxy

    mockStore = mock[ConsistentStore]
    mockMasterOpenSessionAction = mock[Action]
    mockMasterCloseSessionAction = mock[Action]

    val logIdGenerator = new IdGenerator[Long] {
      def nextId = {
        val id = currentLogId
        currentLogId += 1
        id
      }
    }
    sessionManager = new SlaveReplicationSessionManager(service, mockStore, sessionTimeout, commitFrequency = 0, logIdGenerator) {
      override def nextId = currentCookie
    }
    sessionManager.start()
  }

  after {
    sessionManager.stop()
    sessionManager = null
    mockMasterCloseSessionAction = null
    mockMasterOpenSessionAction = null
    mockStore = null
    txLogProxy = null

    sessionEndErrors = Nil
    sessionEndCalls = 0
    pushTxReplyCount = 0
  }

  def onSessionEnd(exception: Option[Exception]) {
    sessionEndCalls += 1
    exception match {
      case Some(e) => sessionEndErrors = e :: sessionEndErrors
      case _ =>
    }
  }

  def verifyOnSessionEnd(expectedCalls: Int, expectedErrors: Exception*) {
    sessionEndCalls should be(expectedCalls)
    sessionEndErrors should be(expectedErrors)
  }

  def verifyOnSessionEndNotCalled() {
    verifyOnSessionEnd(expectedCalls = 0)
  }

  def openSession(startTimestamp: Timestamp = 0L, cookie: String = UUID.randomUUID().toString): ReplicationSession = {
    val endTimestamp = Timestamp(5678)
    val sessionId = "9876"

    currentCookie = cookie
    when(txLogProxy.getLastLoggedRecord).thenReturn(Some(Index(-1, Some(startTimestamp))))
    sessionManager.openSession(member, txLogProxy, delay = 0, ActionProxy(mockMasterOpenSessionAction), ActionProxy(mockMasterCloseSessionAction),
      ReplicationMode.Bootstrap, onSessionEnd)
    val openSessionRequest: Map[String, MValue] = Map(
      ReplicationAPIParams.Token -> token.toString,
      ReplicationAPIParams.Cookie -> cookie,
      ReplicationAPIParams.Start -> startTimestamp.toString,
      ReplicationAPIParams.Mode -> ReplicationMode.Bootstrap.toString)

    val openSessionResponse: Map[String, MValue] = Map(
      ReplicationAPIParams.SessionId -> sessionId,
      ReplicationAPIParams.Cookie -> cookie,
      ReplicationAPIParams.Start -> startTimestamp.toString,
      ReplicationAPIParams.End -> endTimestamp.toString)

    // Verify session is activated
    val matchCaptor = MessageMatcher(openSessionRequest)
    verify(mockMasterOpenSessionAction, timeout(100)).callOutgoingHandlers(argThat(matchCaptor))
    matchCaptor.replyCapturedMessageWith(openSessionResponse)
    verifyNoMoreInteractionsAfter(wait = 100, mockMasterOpenSessionAction)
    verifyZeroInteractions(mockMasterCloseSessionAction)

    sessionManager.sessions.find(_.cookie == cookie).get
  }

  def createTransactionMessage(sequence: Long, sessionId: String, transaction: Message): InMessage = {
    val params: Map[String, MValue] = Map(
      ReplicationAPIParams.Sequence -> sequence.toString,
      ReplicationAPIParams.SessionId -> sessionId,
      ReplicationAPIParams.Timestamp -> transaction.timestamp.get.toString)
    val message = new InMessage(params, data = transaction)
    message.replyCallback = (_) => pushTxReplyCount += 1
    message
  }

  def createKeepAliveMessage(sequence: Long, sessionId: String): InMessage = {
    val params: Map[String, MValue] = Map(
      ReplicationAPIParams.Sequence -> sequence.toString,
      ReplicationAPIParams.SessionId -> sessionId)
    val message = new InMessage(params)
    message.replyCallback = (_) => pushTxReplyCount += 1
    message
  }

  def createSession(cookie: String, mode: ReplicationMode, id: Option[String] = None,
                    startTimestamp: Option[Timestamp] = None, endTimestamp: Option[Timestamp] = None,
                    slave: Node = localNode, member: ResolvedServiceMember = member) = {
    ReplicationSession(member, cookie, mode, localNode, id, startTimestamp, endTimestamp)
  }

  test("open session should invoke master") {
    val startTimestamp = Timestamp(1234)
    val cookie = "miam"

    currentCookie = cookie
    when(txLogProxy.getLastLoggedRecord).thenReturn(Some(Index(-1, Some(startTimestamp))))
    sessionManager.openSession(member, txLogProxy, delay = 0, ActionProxy(mockMasterOpenSessionAction), ActionProxy(mockMasterCloseSessionAction),
      ReplicationMode.Bootstrap, onSessionEnd)
    val openSessionRequest: Map[String, MValue] = Map(
      ReplicationAPIParams.Token -> token.toString,
      ReplicationAPIParams.Cookie -> cookie,
      ReplicationAPIParams.Start -> startTimestamp.toString,
      ReplicationAPIParams.Mode -> ReplicationMode.Bootstrap.toString)

    verify(mockMasterOpenSessionAction, timeout(100)).callOutgoingHandlers(argThat(matchMessage(openSessionRequest)))
    verifyNoMoreInteractions(mockMasterOpenSessionAction)
    verifyZeroInteractions(mockMasterCloseSessionAction)
    verifyOnSessionEndNotCalled()
    sessionManager.sessions should be(List(createSession(cookie, ReplicationMode.Bootstrap)))
  }

  test("open session should invoke master only after delay") {
    val startTimestamp = Timestamp(1234)
    val cookie = "miam"

    currentCookie = cookie
    when(txLogProxy.getLastLoggedRecord).thenReturn(Some(Index(-1, Some(startTimestamp))))
    sessionManager.openSession(member, txLogProxy, delay = 800, ActionProxy(mockMasterOpenSessionAction), ActionProxy(mockMasterCloseSessionAction),
      ReplicationMode.Bootstrap, onSessionEnd)
    val openSessionRequest: Map[String, MValue] = Map(
      ReplicationAPIParams.Token -> token.toString,
      ReplicationAPIParams.Cookie -> cookie,
      ReplicationAPIParams.Start -> startTimestamp.toString,
      ReplicationAPIParams.Mode -> ReplicationMode.Bootstrap.toString)

    // Wait 75% of the delay and ensure no interaction yet
    verifyZeroInteractionsAfter(wait = 600, mockMasterOpenSessionAction)
    verifyZeroInteractions(mockMasterCloseSessionAction)
    verifyOnSessionEndNotCalled()
    sessionManager.sessions should be(List(createSession(cookie, ReplicationMode.Bootstrap)))

    // Wait delay remainder and verify subscribe call is done
    verify(mockMasterOpenSessionAction, timeout(200)).callOutgoingHandlers(argThat(matchMessage(openSessionRequest)))
    verifyNoMoreInteractions(mockMasterOpenSessionAction)
    verifyZeroInteractions(mockMasterCloseSessionAction)
    verifyOnSessionEndNotCalled()
    sessionManager.sessions should be(List(createSession(cookie, ReplicationMode.Bootstrap)))
  }

  test("open session should not invoke master twice if a session is already pending") {
    val startTimestamp = Timestamp(1234)
    val cookie = "miam"
    val expectedSession = createSession(cookie, ReplicationMode.Bootstrap)

    currentCookie = cookie
    when(txLogProxy.getLastLoggedRecord).thenReturn(Some(Index(-1, Some(startTimestamp))))
    sessionManager.openSession(member, txLogProxy, delay = 800, ActionProxy(mockMasterOpenSessionAction), ActionProxy(mockMasterCloseSessionAction),
      ReplicationMode.Bootstrap, onSessionEnd)
    val openSessionRequest: Map[String, MValue] = Map(
      ReplicationAPIParams.Token -> token.toString,
      ReplicationAPIParams.Cookie -> cookie,
      ReplicationAPIParams.Start -> startTimestamp.toString,
      ReplicationAPIParams.Mode -> ReplicationMode.Bootstrap.toString)

    // Waiting 50% of the delay
    verifyZeroInteractionsAfter(wait = 400, mockMasterOpenSessionAction)
    verifyZeroInteractions(mockMasterCloseSessionAction)
    verifyOnSessionEndNotCalled()
    sessionManager.sessions should be(List(expectedSession))

    // Try subscribe again
    sessionManager.openSession(member, txLogProxy, delay = 0, ActionProxy(mockMasterOpenSessionAction), ActionProxy(mockMasterCloseSessionAction),
      ReplicationMode.Bootstrap, onSessionEnd)

    // Wait delay remainder and verify subscribe call is done only once
    Thread.sleep(400)
    verify(mockMasterOpenSessionAction, timeout(100)).callOutgoingHandlers(argThat(matchMessage(openSessionRequest)))
    verifyNoMoreInteractions(mockMasterOpenSessionAction)
    verifyZeroInteractions(mockMasterCloseSessionAction)
    verifyOnSessionEndNotCalled()
    sessionManager.sessions should be(List(expectedSession))
  }

  test("session should be activated after receiving an open session response") {
    val startTimestamp = Timestamp(1234)
    val endTimestamp = Timestamp(5678)
    val cookie = "miam"
    val sessionId = "9876"
    val expectedSession = createSession(cookie, ReplicationMode.Bootstrap, Some(sessionId),
      Some(startTimestamp), Some(endTimestamp))

    currentCookie = cookie
    when(txLogProxy.getLastLoggedRecord).thenReturn(Some(Index(-1, Some(startTimestamp))))
    sessionManager.openSession(member, txLogProxy, delay = 0, ActionProxy(mockMasterOpenSessionAction), ActionProxy(mockMasterCloseSessionAction),
      ReplicationMode.Bootstrap, onSessionEnd)
    val openSessionRequest: Map[String, MValue] = Map(
      ReplicationAPIParams.Token -> token.toString,
      ReplicationAPIParams.Cookie -> cookie,
      ReplicationAPIParams.Start -> startTimestamp.toString,
      ReplicationAPIParams.Mode -> ReplicationMode.Bootstrap.toString)

    val openSessionResponse: Map[String, MValue] = Map(
      ReplicationAPIParams.SessionId -> sessionId,
      ReplicationAPIParams.Cookie -> cookie,
      ReplicationAPIParams.Start -> startTimestamp.toString,
      ReplicationAPIParams.End -> endTimestamp.toString)


    val matchCaptor = MessageMatcher(openSessionRequest)
    verify(mockMasterOpenSessionAction, timeout(100)).callOutgoingHandlers(argThat(matchCaptor))
    matchCaptor.replyCapturedMessageWith(openSessionResponse)
    verifyNoMoreInteractionsAfter(wait = 100, mockMasterOpenSessionAction)
    verifyZeroInteractions(mockMasterCloseSessionAction)
    verifyOnSessionEndNotCalled()
    sessionManager.sessions should be(List(expectedSession))
  }

  test("session should NOT be activated if receiving wrong cookie in the open session response") {
    val startTimestamp = Timestamp(1234)
    val endTimestamp = Timestamp(5678)
    val cookie = "miam"
    val sessionId = "9876"
    val expectedSession = createSession(cookie, ReplicationMode.Bootstrap)

    currentCookie = cookie
    when(txLogProxy.getLastLoggedRecord).thenReturn(Some(Index(-1, Some(startTimestamp))))
    sessionManager.openSession(member, txLogProxy, delay = 0, ActionProxy(mockMasterOpenSessionAction), ActionProxy(mockMasterCloseSessionAction),
      ReplicationMode.Bootstrap, onSessionEnd)
    val openSessionRequest: Map[String, MValue] = Map(
      ReplicationAPIParams.Token -> token.toString,
      ReplicationAPIParams.Cookie -> cookie,
      ReplicationAPIParams.Start -> startTimestamp.toString,
      ReplicationAPIParams.Mode -> ReplicationMode.Bootstrap.toString)

    val openSessionResponse: Map[String, MValue] = Map(
      ReplicationAPIParams.SessionId -> sessionId,
      ReplicationAPIParams.Cookie -> "bad cookie",
      ReplicationAPIParams.Start -> startTimestamp.toString,
      ReplicationAPIParams.End -> endTimestamp.toString)

    val closeSessionRequest: Map[String, MValue] = Map(
      ReplicationAPIParams.Token -> token.toString,
      ReplicationAPIParams.SessionId -> sessionId)

    val matchCaptor = MessageMatcher(openSessionRequest)
    verify(mockMasterOpenSessionAction, timeout(100)).callOutgoingHandlers(argThat(matchCaptor))
    matchCaptor.replyCapturedMessageWith(openSessionResponse)
    verifyNoMoreInteractionsAfter(wait = 100, mockMasterOpenSessionAction)
    verify(mockMasterCloseSessionAction, timeout(100)).callOutgoingHandlers(argThat(matchMessage(closeSessionRequest)))
    verifyZeroInteractions(mockMasterCloseSessionAction)
    verifyOnSessionEndNotCalled()
    sessionManager.sessions should be(List(expectedSession))
  }

  test("open session response error should remove pending session") {
    val startTimestamp = Timestamp(1234)
    val cookie = "miam"

    currentCookie = cookie
    when(txLogProxy.getLastLoggedRecord).thenReturn(Some(Index(-1, Some(startTimestamp))))
    sessionManager.openSession(member, txLogProxy, delay = 0, ActionProxy(mockMasterOpenSessionAction), ActionProxy(mockMasterCloseSessionAction),
      ReplicationMode.Bootstrap, onSessionEnd)
    val openSessionRequest: Map[String, MValue] = Map(
      ReplicationAPIParams.Token -> token.toString,
      ReplicationAPIParams.Cookie -> cookie,
      ReplicationAPIParams.Start -> startTimestamp.toString,
      ReplicationAPIParams.Mode -> ReplicationMode.Bootstrap.toString)

    val openSessionResponse = new Exception("reponse error")

    // Verify onSessionEnd is invoked with the error
    val matchCaptor = MessageMatcher(openSessionRequest)
    verify(mockMasterOpenSessionAction, timeout(100)).callOutgoingHandlers(argThat(matchCaptor))
    matchCaptor.replyCapturedMessageWith(openSessionResponse)
    verifyNoMoreInteractionsAfter(wait = 100, mockMasterOpenSessionAction)
    verifyZeroInteractions(mockMasterCloseSessionAction)
    verifyOnSessionEnd(1, openSessionResponse)
    sessionManager.sessions should be(Nil)
  }

  test("open session response with identical start/end timestamps should not active session") {
    val startTimestamp = Timestamp(1234)
    val cookie = "miam"
    val sessionId = "9876"

    currentCookie = cookie
    when(txLogProxy.getLastLoggedRecord).thenReturn(Some(Index(-1, Some(startTimestamp))))
    sessionManager.openSession(member, txLogProxy, delay = 0, ActionProxy(mockMasterOpenSessionAction), ActionProxy(mockMasterCloseSessionAction),
      ReplicationMode.Bootstrap, onSessionEnd)
    val openSessionRequest: Map[String, MValue] = Map(
      ReplicationAPIParams.Token -> token.toString,
      ReplicationAPIParams.Cookie -> cookie,
      ReplicationAPIParams.Start -> startTimestamp.toString,
      ReplicationAPIParams.Mode -> ReplicationMode.Bootstrap.toString)

    val openSessionResponse: Map[String, MValue] = Map(
      ReplicationAPIParams.SessionId -> sessionId,
      ReplicationAPIParams.Cookie -> cookie,
      ReplicationAPIParams.Start -> startTimestamp.toString,
      ReplicationAPIParams.End -> startTimestamp.toString)

    val closeSessionRequest: Map[String, MValue] = Map(
      ReplicationAPIParams.Token -> token.toString,
      ReplicationAPIParams.SessionId -> sessionId)

    // Verify close session is sent and onSessionEnd is invoked without an error
    val matchCaptor = MessageMatcher(openSessionRequest)
    verify(mockMasterOpenSessionAction, timeout(100)).callOutgoingHandlers(argThat(matchCaptor))
    matchCaptor.replyCapturedMessageWith(openSessionResponse)
    verifyNoMoreInteractionsAfter(wait = 100, mockMasterOpenSessionAction)
    verify(mockMasterCloseSessionAction, timeout(100)).callOutgoingHandlers(argThat(matchMessage(closeSessionRequest)))
    verifyZeroInteractions(mockMasterCloseSessionAction)
    verifyOnSessionEnd(1)
    sessionManager.sessions should be(Nil)
  }

  test("open session should not invoke master twice if already have a session") {
    val startTimestamp = Timestamp(1234)
    val endTimestamp = Timestamp(5678)
    val cookie = "miam"
    val sessionId = "9876"
    val expectedSession = createSession(cookie, ReplicationMode.Bootstrap, Some(sessionId),
      Some(startTimestamp), Some(endTimestamp))

    currentCookie = cookie
    when(txLogProxy.getLastLoggedRecord).thenReturn(Some(Index(-1, Some(startTimestamp))))
    sessionManager.openSession(member, txLogProxy, delay = 0, ActionProxy(mockMasterOpenSessionAction), ActionProxy(mockMasterCloseSessionAction),
      ReplicationMode.Bootstrap, onSessionEnd)
    val openSessionRequest: Map[String, MValue] = Map(
      ReplicationAPIParams.Token -> token.toString,
      ReplicationAPIParams.Cookie -> cookie,
      ReplicationAPIParams.Start -> startTimestamp.toString,
      ReplicationAPIParams.Mode -> ReplicationMode.Bootstrap.toString)

    val openSessionResponse: Map[String, MValue] = Map(
      ReplicationAPIParams.SessionId -> sessionId,
      ReplicationAPIParams.Cookie -> cookie,
      ReplicationAPIParams.Start -> startTimestamp.toString,
      ReplicationAPIParams.End -> endTimestamp.toString)

    val matchCaptor = MessageMatcher(openSessionRequest)
    verify(mockMasterOpenSessionAction, timeout(100)).callOutgoingHandlers(argThat(matchCaptor))
    matchCaptor.replyCapturedMessageWith(openSessionResponse)
    verifyNoMoreInteractionsAfter(wait = 100, mockMasterOpenSessionAction)
    verifyZeroInteractions(mockMasterCloseSessionAction)
    verifyOnSessionEndNotCalled()
    sessionManager.sessions should be(List(expectedSession))

    sessionManager.openSession(member, txLogProxy, delay = 0, ActionProxy(mockMasterOpenSessionAction), ActionProxy(mockMasterCloseSessionAction),
      ReplicationMode.Bootstrap, onSessionEnd)
    verifyZeroInteractionsAfter(wait = 100, mockMasterOpenSessionAction, mockMasterCloseSessionAction)
    verifyOnSessionEndNotCalled()
    sessionManager.sessions should be(List(expectedSession))
  }

  test("open session when have another service member session pending") {
    val startTimestamp = Timestamp(1234)
    val cookie = "miam"
    val expectedSession = createSession(cookie, ReplicationMode.Bootstrap)

    // First session with a different service member
    val otherMember = ResolvedServiceMember(service.name, token + 1000, Seq(TokenRange.All))
    val otherCookie = "other"
    val otherSession = createSession(otherCookie, ReplicationMode.Bootstrap, member = otherMember)
    currentCookie = otherCookie
    when(txLogProxy.getLastLoggedRecord).thenReturn(Some(Index(-1, Some(startTimestamp))))
    sessionManager.openSession(otherMember, txLogProxy, delay = 5000, ActionProxy(mockMasterOpenSessionAction), ActionProxy(mockMasterCloseSessionAction),
      ReplicationMode.Bootstrap, onSessionEnd)
    sessionManager.sessions should be(List(otherSession))
    verifyZeroInteractions(mockMasterOpenSessionAction, mockMasterCloseSessionAction)

    // Open new replication session
    currentCookie = cookie
    when(txLogProxy.getLastLoggedRecord).thenReturn(Some(Index(-1, Some(startTimestamp))))
    sessionManager.openSession(member, txLogProxy, delay = 0, ActionProxy(mockMasterOpenSessionAction), ActionProxy(mockMasterCloseSessionAction),
      ReplicationMode.Bootstrap, onSessionEnd)
    val openSessionRequest: Map[String, MValue] = Map(
      ReplicationAPIParams.Token -> token.toString,
      ReplicationAPIParams.Cookie -> cookie,
      ReplicationAPIParams.Start -> startTimestamp.toString,
      ReplicationAPIParams.Mode -> ReplicationMode.Bootstrap.toString)

    // Validate now have two pending sessions
    verify(mockMasterOpenSessionAction, timeout(100)).callOutgoingHandlers(argThat(matchMessage(openSessionRequest)))
    verifyNoMoreInteractions(mockMasterOpenSessionAction)
    verifyZeroInteractions(mockMasterCloseSessionAction)
    verifyOnSessionEndNotCalled()
    sessionManager.sessions.toSet should be(Set(otherSession, expectedSession))
  }

  test("close pending session before pending delay expires should should not invoke master") {
    val startTimestamp = Timestamp(1234)
    val cookie = "miam"

    currentCookie = cookie
    when(txLogProxy.getLastLoggedRecord).thenReturn(Some(Index(-1, Some(startTimestamp))))
    sessionManager.openSession(member, txLogProxy, delay = 500, ActionProxy(mockMasterOpenSessionAction), ActionProxy(mockMasterCloseSessionAction),
      ReplicationMode.Bootstrap, onSessionEnd)

    // Ensure no interaction yet
    verifyZeroInteractionsAfter(wait = 200, mockMasterOpenSessionAction, mockMasterCloseSessionAction)
    verifyOnSessionEndNotCalled()
    sessionManager.sessions should be(List(createSession(cookie, ReplicationMode.Bootstrap)))

    // Close session and wait end of the delay
    sessionManager.closeSession(member)
    verifyZeroInteractionsAfter(wait = 400, mockMasterOpenSessionAction, mockMasterCloseSessionAction)
    verifyOnSessionEndNotCalled()
    sessionManager.sessions should be(Nil)
  }

  test("close pending session should ignore open session response") {
    val startTimestamp = Timestamp(1234)
    val cookie = "miam"
    val sessionId = "9876"
    val expectedSession = createSession(cookie, ReplicationMode.Bootstrap)

    currentCookie = cookie
    when(txLogProxy.getLastLoggedRecord).thenReturn(Some(Index(-1, Some(startTimestamp))))
    sessionManager.openSession(member, txLogProxy, delay = 0, ActionProxy(mockMasterOpenSessionAction), ActionProxy(mockMasterCloseSessionAction),
      ReplicationMode.Bootstrap, onSessionEnd)

    val openSessionRequest: Map[String, MValue] = Map(
      ReplicationAPIParams.Token -> token.toString,
      ReplicationAPIParams.Cookie -> cookie,
      ReplicationAPIParams.Start -> startTimestamp.toString,
      ReplicationAPIParams.Mode -> ReplicationMode.Bootstrap.toString)

    val openSessionResponse: Map[String, MValue] = Map(
      ReplicationAPIParams.SessionId -> sessionId,
      ReplicationAPIParams.Cookie -> cookie,
      ReplicationAPIParams.Start -> startTimestamp.toString,
      ReplicationAPIParams.End -> "5678")

    val closeSessionRequest: Map[String, MValue] = Map(
      ReplicationAPIParams.Token -> token.toString,
      ReplicationAPIParams.SessionId -> sessionId)

    // Verify session is pending (open session reponse is delayed after call to openSession)
    val matchCaptor = MessageMatcher(openSessionRequest)
    verify(mockMasterOpenSessionAction, timeout(100)).callOutgoingHandlers(argThat(matchCaptor))
    verifyNoMoreInteractionsAfter(wait = 100, mockMasterOpenSessionAction)
    verifyZeroInteractions(mockMasterCloseSessionAction)
    verifyOnSessionEndNotCalled()
    sessionManager.sessions should be(List(expectedSession))

    // close pending session before receiving response
    sessionManager.closeSession(member)
    verifyZeroInteractionsAfter(wait = 100, mockMasterOpenSessionAction)
    verifyZeroInteractions(mockMasterCloseSessionAction)
    verifyOnSessionEndNotCalled()
    sessionManager.sessions should be(Nil)

    // Finally receive the open session response (after calling closeSession)
    matchCaptor.replyCapturedMessageWith(openSessionResponse)

    // Verify close session response is sent after getting the late open session response
    verify(mockMasterCloseSessionAction, timeout(100)).callOutgoingHandlers(argThat(matchMessage(closeSessionRequest)))
    verifyZeroInteractionsAfter(wait = 100, mockMasterOpenSessionAction, mockMasterCloseSessionAction)
    verifyOnSessionEndNotCalled()
    sessionManager.sessions should be(Nil)
  }

  test("close session should close the proper session") {
    val startTimestamp = Timestamp(1234)
    val endTimestamp = Timestamp(5678)
    val cookie = "miam"
    val sessionId = "9876"
    val expectedSession = createSession(cookie, ReplicationMode.Bootstrap, Some(sessionId),
      Some(startTimestamp), Some(endTimestamp))

    currentCookie = cookie
    when(txLogProxy.getLastLoggedRecord).thenReturn(Some(Index(-1, Some(startTimestamp))))
    sessionManager.openSession(member, txLogProxy, delay = 0, ActionProxy(mockMasterOpenSessionAction), ActionProxy(mockMasterCloseSessionAction),
      ReplicationMode.Bootstrap, onSessionEnd)
    val openSessionRequest: Map[String, MValue] = Map(
      ReplicationAPIParams.Token -> token.toString,
      ReplicationAPIParams.Cookie -> cookie,
      ReplicationAPIParams.Start -> startTimestamp.toString,
      ReplicationAPIParams.Mode -> ReplicationMode.Bootstrap.toString)

    val openSessionResponse: Map[String, MValue] = Map(
      ReplicationAPIParams.SessionId -> sessionId,
      ReplicationAPIParams.Cookie -> cookie,
      ReplicationAPIParams.Start -> startTimestamp.toString,
      ReplicationAPIParams.End -> endTimestamp.toString)

    // Verify session is activated
    val matchCaptor = MessageMatcher(openSessionRequest)
    verify(mockMasterOpenSessionAction, timeout(100)).callOutgoingHandlers(argThat(matchCaptor))
    matchCaptor.replyCapturedMessageWith(openSessionResponse)
    verifyNoMoreInteractionsAfter(wait = 100, mockMasterOpenSessionAction)
    verifyZeroInteractions(mockMasterCloseSessionAction)
    verifyOnSessionEndNotCalled()
    sessionManager.sessions should be(List(expectedSession))

    // Calling closeSession for a member without session should do nothing
    val otherMember = ResolvedServiceMember(service.name, token + 1000, Seq(TokenRange.All))
    sessionManager.closeSession(otherMember)
    verifyZeroInteractionsAfter(wait = 100, mockMasterCloseSessionAction)
    verifyZeroInteractions(mockMasterOpenSessionAction)
    sessionManager.sessions should be(List(expectedSession))

    // Closing the active session
    val closeSessionRequest: Map[String, MValue] = Map(
      ReplicationAPIParams.Token -> token.toString,
      ReplicationAPIParams.SessionId -> sessionId)

    sessionManager.closeSession(member)
    verify(mockMasterCloseSessionAction, timeout(100)).callOutgoingHandlers(argThat(matchMessage(closeSessionRequest)))
    verifyZeroInteractionsAfter(wait = 100, mockMasterCloseSessionAction, mockMasterOpenSessionAction)
    sessionManager.sessions should be(Nil)
  }

  test("replication message should store in tx log and store in proper order") {
    val startTimestamp = 0L
    val session = openSession(startTimestamp)

    val tx1 = createRequestMessage(timestamp = 100L)
    val tx2 = createRequestMessage(timestamp = 200L)
    val tx3 = createRequestMessage(timestamp = 300L)

    val ordered = inOrder(txLogProxy.mockAppender, mockStore)
    currentLogId = 1000

    // Intentionally replicate transactions out of order
    sessionManager.handleReplicationMessage(createTransactionMessage(sequence = 2, session.id.get, tx2))
    sessionManager.handleReplicationMessage(createTransactionMessage(sequence = 3, session.id.get, tx3))
    sessionManager.handleReplicationMessage(createTransactionMessage(sequence = 1, session.id.get, tx1))

    // Wait for transaction processing, unfortunately timeout mode is not implemented with InOrder
    Thread.sleep(100)

    // Verify transactions applied in order
    ordered.verify(txLogProxy.mockAppender).append(LogRecord(1000, Some(startTimestamp), tx1))
    ordered.verify(mockStore).writeTransaction(argThat(matchMessage(tx1)))
    ordered.verify(txLogProxy.mockAppender).append(LogRecord(1001, Some(startTimestamp), createResponseMessage(tx1)))
    ordered.verify(txLogProxy.mockAppender).append(LogRecord(1002, tx1.timestamp, tx2))
    ordered.verify(mockStore).writeTransaction(argThat(matchMessage(tx2)))
    ordered.verify(txLogProxy.mockAppender).append(LogRecord(1003, tx1.timestamp, createResponseMessage(tx2)))
    ordered.verify(txLogProxy.mockAppender).append(LogRecord(1004, tx2.timestamp, tx3))
    ordered.verify(mockStore).writeTransaction(argThat(matchMessage(tx3)))
    ordered.verify(txLogProxy.mockAppender).append(LogRecord(1005, tx2.timestamp, createResponseMessage(tx3)))
    verifyNoMoreInteractions(txLogProxy.mockAppender, mockStore)
    pushTxReplyCount should be(3)
    verifyZeroInteractions(mockMasterOpenSessionAction, mockMasterCloseSessionAction)
  }

  test("replication message tx log append error should result in consistency error") {
    val startTimestamp = 0L
    val session = openSession(startTimestamp)

    currentLogId = 1000

    val tx1 = createRequestMessage(timestamp = 100L)
    val tx2 = createRequestMessage(timestamp = 200L)
    val tx3 = createRequestMessage(timestamp = 300L)

    val ordered = inOrder(txLogProxy.mockAppender, mockStore)
    when(txLogProxy.mockAppender.append(LogRecord(1002, tx1.timestamp, tx2))).thenThrow(new RuntimeException())

    // Intentionally replicate transactions out of order
    sessionManager.handleReplicationMessage(createTransactionMessage(sequence = 2, session.id.get, tx2))
    sessionManager.handleReplicationMessage(createTransactionMessage(sequence = 1, session.id.get, tx1))
    sessionManager.handleReplicationMessage(createTransactionMessage(sequence = 3, session.id.get, tx3))

    // Wait for transaction processing, unfortunately timeout mode is not implemented with InOrder
    Thread.sleep(100)

    // Verify transactions applied in order up to the error
    ordered.verify(txLogProxy.mockAppender).append(LogRecord(1000, Some(startTimestamp), tx1))
    ordered.verify(mockStore).writeTransaction(argThat(matchMessage(tx1)))
    ordered.verify(txLogProxy.mockAppender).append(LogRecord(1001, Some(startTimestamp), createResponseMessage(tx1)))
    ordered.verify(txLogProxy.mockAppender).append(LogRecord(1002, tx1.timestamp, tx2))
    verifyNoMoreInteractions(txLogProxy.mockAppender, mockStore)
    pushTxReplyCount should be(1)

    val closeSessionRequest: Map[String, MValue] = Map(
      ReplicationAPIParams.Token -> token.toString,
      ReplicationAPIParams.SessionId -> session.id.get)
    verify(mockMasterCloseSessionAction, timeout(100)).callOutgoingHandlers(argThat(matchMessage(closeSessionRequest)))
    verifyZeroInteractions(mockMasterOpenSessionAction)
  }

  test("replication message consistent store write error should result in consistency error") {
    val startTimestamp = 0L
    val session = openSession(startTimestamp)

    currentLogId = 1000

    val tx1 = createRequestMessage(timestamp = 100L)
    val tx2 = createRequestMessage(timestamp = 200L)

    val ordered = inOrder(txLogProxy.mockAppender, mockStore)
    when(mockStore.writeTransaction(argThat(matchMessage(tx1)))).thenThrow(new RuntimeException())

    // Intentionally replicate transactions out of order
    sessionManager.handleReplicationMessage(createTransactionMessage(sequence = 2, session.id.get, tx2))
    sessionManager.handleReplicationMessage(createTransactionMessage(sequence = 1, session.id.get, tx1))

    // Wait for transaction processing, unfortunatly timeout mode is not implemented with InOrder
    Thread.sleep(100)

    // Verify transactions applied in order up to the error
    ordered.verify(txLogProxy.mockAppender).append(LogRecord(1000, Some(startTimestamp), tx1))
    ordered.verify(mockStore).writeTransaction(argThat(matchMessage(tx1)))
    verifyNoMoreInteractions(txLogProxy.mockAppender, mockStore)
    pushTxReplyCount should be(0)

    val closeSessionRequest: Map[String, MValue] = Map(
      ReplicationAPIParams.Token -> token.toString,
      ReplicationAPIParams.SessionId -> session.id.get)
    verify(mockMasterCloseSessionAction, timeout(100)).callOutgoingHandlers(argThat(matchMessage(closeSessionRequest)))
    verifyZeroInteractions(mockMasterOpenSessionAction)
  }

  test("skip replication message sequence # should result in consistency error") {
    val startTimestamp = 0L
    val session = openSession(startTimestamp)

    currentLogId = 1000

    val tx1 = createRequestMessage(timestamp = 100L)
    val tx2 = createRequestMessage(timestamp = 200L)
    val tx3 = createRequestMessage(timestamp = 300L)

    val ordered = inOrder(txLogProxy.mockAppender, mockStore)

    // Replicate transactions with a sequence # gap until timeout and resume
    sessionManager.handleReplicationMessage(createTransactionMessage(sequence = 3, session.id.get, tx3))
    sessionManager.handleReplicationMessage(createTransactionMessage(sequence = 1, session.id.get, tx1))
    Thread.sleep((sessionTimeout * 1.5).toLong)
    sessionManager.handleReplicationMessage(createTransactionMessage(sequence = 2, session.id.get, tx2))

    // Verify transactions applied in order up to the timeout
    ordered.verify(txLogProxy.mockAppender).append(LogRecord(1000, Some(startTimestamp), tx1))
    ordered.verify(mockStore).writeTransaction(argThat(matchMessage(tx1)))
    ordered.verify(txLogProxy.mockAppender).append(LogRecord(1001, Some(startTimestamp), createResponseMessage(tx1)))
    verifyNoMoreInteractions(txLogProxy.mockAppender, mockStore)
    pushTxReplyCount should be(1)

    val closeSessionRequest: Map[String, MValue] = Map(
      ReplicationAPIParams.Token -> token.toString,
      ReplicationAPIParams.SessionId -> session.id.get)
    verify(mockMasterCloseSessionAction, timeout(100)).callOutgoingHandlers(argThat(matchMessage(closeSessionRequest)))
    verifyZeroInteractions(mockMasterOpenSessionAction)
  }

  test("stop receiving replication message should result in timeout error") {
    val startTimestamp = 0L
    val session = openSession(startTimestamp)

    currentLogId = 1000

    val tx1 = createRequestMessage(timestamp = 100L)

    val ordered = inOrder(txLogProxy.mockAppender, mockStore)

    // Publish transactions with a sequence # gap until timeout and resume
    sessionManager.handleReplicationMessage(createTransactionMessage(sequence = 1, session.id.get, tx1))
    Thread.sleep((sessionTimeout * 1.5).toLong)

    // Verify transactions applied in order up to the sequence # gap
    ordered.verify(txLogProxy.mockAppender).append(LogRecord(1000, Some(startTimestamp), tx1))
    ordered.verify(mockStore).writeTransaction(argThat(matchMessage(tx1)))
    ordered.verify(txLogProxy.mockAppender).append(LogRecord(1001, Some(startTimestamp), createResponseMessage(tx1)))
    verifyNoMoreInteractions(txLogProxy.mockAppender, mockStore)
    pushTxReplyCount should be(1)

    val closeSessionRequest: Map[String, MValue] = Map(
      ReplicationAPIParams.Token -> token.toString,
      ReplicationAPIParams.SessionId -> session.id.get)
    verify(mockMasterCloseSessionAction, timeout(100)).callOutgoingHandlers(argThat(matchMessage(closeSessionRequest)))
    verifyZeroInteractions(mockMasterOpenSessionAction)
  }

  test("idle message should not result in timeout error") {
    val startTimestamp = 0L
    val session = openSession(startTimestamp)
    currentLogId = 1000

    // Publish keep alive messages
    sessionManager.handleReplicationMessage(createKeepAliveMessage(sequence = 1, session.id.get))
    Thread.sleep(sessionTimeout / 2)
    sessionManager.handleReplicationMessage(createKeepAliveMessage(sequence = 2, session.id.get))
    Thread.sleep(sessionTimeout / 2)
    sessionManager.handleReplicationMessage(createKeepAliveMessage(sequence = 3, session.id.get))
    Thread.sleep(sessionTimeout / 2)

    // Wait for transaction processing
    Thread.sleep(100)

    verifyZeroInteractions(txLogProxy.mockAppender, mockStore, mockMasterOpenSessionAction, mockMasterCloseSessionAction)
    pushTxReplyCount should be(3)
  }
}