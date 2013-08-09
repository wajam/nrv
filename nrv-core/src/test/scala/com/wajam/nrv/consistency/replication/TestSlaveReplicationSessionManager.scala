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
import com.wajam.nrv.utils.timestamp.Timestamp
import MessageMatcher._
import com.wajam.nrv.consistency.persistence.LogRecord.Index
import java.util.UUID
import com.wajam.nrv.utils.IdGenerator
import com.wajam.nrv.consistency.persistence.{TransactionLogProxy, LogRecord}

@RunWith(classOf[JUnitRunner])
class TestSlaveReplicationSessionManager extends TestTransactionBase with BeforeAndAfter with MockitoSugar {

  var service: Service = null
  var member: ResolvedServiceMember = null
  var txLogProxy: TransactionLogProxy = null
  var mockStore: ConsistentStore = null
  var mockSubscribeAction: Action = null
  var mockUnsubscribeAction: Action = null

  var currentCookie: String = null
  var currentLogId: Long = 0
  var sessionManager: SlaveReplicationSessionManager = null

  val sessionTimeout = 1500L
  val token = 0
  var sessionEndErrors: List[Exception] = Nil
  var sessionEndCalls = 0
  var pushTxReplyCount = 0

  before {
    service = new Service("service")
    member = ResolvedServiceMember(service.name, token, Seq(TokenRange.All))
    txLogProxy = new TransactionLogProxy

    mockStore = mock[ConsistentStore]
    mockSubscribeAction = mock[Action]
    mockUnsubscribeAction = mock[Action]

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
    mockUnsubscribeAction = null
    mockSubscribeAction = null
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

  def subscribe(startTimestamp: Timestamp = 0L, cookie: String = UUID.randomUUID().toString): ReplicationSession = {
    val endTimestamp = Timestamp(5678)
    val sessionId = "9876"

    currentCookie = cookie
    when(txLogProxy.getLastLoggedRecord).thenReturn(Some(Index(-1, Some(startTimestamp))))
    sessionManager.subscribe(member, txLogProxy, delay = 0, ActionProxy(mockSubscribeAction), ActionProxy(mockUnsubscribeAction),
      ReplicationMode.Bootstrap, onSessionEnd)
    val subscribeRequest: Map[String, MValue] = Map(
      ReplicationAPIParams.Token -> token.toString,
      ReplicationAPIParams.Cookie -> cookie,
      ReplicationAPIParams.Start -> startTimestamp.toString,
      ReplicationAPIParams.Mode -> ReplicationMode.Store.toString)

    val subscribeResponse: Map[String, MValue] = Map(
      ReplicationAPIParams.SessionId -> sessionId,
      ReplicationAPIParams.Cookie -> cookie,
      ReplicationAPIParams.Start -> startTimestamp.toString,
      ReplicationAPIParams.End -> endTimestamp.toString)

    // Verify session is activated
    val matchCaptor = MessageMatcher(subscribeRequest)
    verify(mockSubscribeAction, timeout(100)).callOutgoingHandlers(argThat(matchCaptor))
    matchCaptor.replyCapturedMessageWith(subscribeResponse)
    verifyNoMoreInteractionsAfter(wait = 100, mockSubscribeAction)
    verifyZeroInteractions(mockUnsubscribeAction)

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

  test("subscribe should invoke master") {
    val startTimestamp = Timestamp(1234)
    val cookie = "miam"

    currentCookie = cookie
    when(txLogProxy.getLastLoggedRecord).thenReturn(Some(Index(-1, Some(startTimestamp))))
    sessionManager.subscribe(member, txLogProxy, delay = 0, ActionProxy(mockSubscribeAction), ActionProxy(mockUnsubscribeAction),
      ReplicationMode.Bootstrap, onSessionEnd)
    val subscribeRequest: Map[String, MValue] = Map(
      ReplicationAPIParams.Token -> token.toString,
      ReplicationAPIParams.Cookie -> cookie,
      ReplicationAPIParams.Start -> startTimestamp.toString,
      ReplicationAPIParams.Mode -> ReplicationMode.Store.toString)

    verify(mockSubscribeAction, timeout(100)).callOutgoingHandlers(argThat(matchMessage(subscribeRequest)))
    verifyNoMoreInteractions(mockSubscribeAction)
    verifyZeroInteractions(mockUnsubscribeAction)
    verifyOnSessionEndNotCalled()
    sessionManager.sessions should be(List(ReplicationSession(member, cookie, ReplicationMode.Bootstrap)))
  }

  test("subscribe should invoke master only after delay") {
    val startTimestamp = Timestamp(1234)
    val cookie = "miam"

    currentCookie = cookie
    when(txLogProxy.getLastLoggedRecord).thenReturn(Some(Index(-1, Some(startTimestamp))))
    sessionManager.subscribe(member, txLogProxy, delay = 800, ActionProxy(mockSubscribeAction), ActionProxy(mockUnsubscribeAction),
      ReplicationMode.Bootstrap, onSessionEnd)
    val subscribeRequest: Map[String, MValue] = Map(
      ReplicationAPIParams.Token -> token.toString,
      ReplicationAPIParams.Cookie -> cookie,
      ReplicationAPIParams.Start -> startTimestamp.toString,
      ReplicationAPIParams.Mode -> ReplicationMode.Store.toString)

    // Wait 75% of the delay and ensure no interaction yet
    verifyZeroInteractionsAfter(wait = 600, mockSubscribeAction)
    verifyZeroInteractions(mockUnsubscribeAction)
    verifyOnSessionEndNotCalled()
    sessionManager.sessions should be(List(ReplicationSession(member, cookie, ReplicationMode.Bootstrap)))

    // Wait delay remainder and verify subscribe call is done
    verify(mockSubscribeAction, timeout(200)).callOutgoingHandlers(argThat(matchMessage(subscribeRequest)))
    verifyNoMoreInteractions(mockSubscribeAction)
    verifyZeroInteractions(mockUnsubscribeAction)
    verifyOnSessionEndNotCalled()
    sessionManager.sessions should be(List(ReplicationSession(member, cookie, ReplicationMode.Bootstrap)))
  }

  test("subscribe should not invoke master twice if a session is already pending") {
    val startTimestamp = Timestamp(1234)
    val cookie = "miam"
    val expectedSession = ReplicationSession(member, cookie, ReplicationMode.Bootstrap)

    currentCookie = cookie
    when(txLogProxy.getLastLoggedRecord).thenReturn(Some(Index(-1, Some(startTimestamp))))
    sessionManager.subscribe(member, txLogProxy, delay = 800, ActionProxy(mockSubscribeAction), ActionProxy(mockUnsubscribeAction),
      ReplicationMode.Bootstrap, onSessionEnd)
    val subscribeRequest: Map[String, MValue] = Map(
      ReplicationAPIParams.Token -> token.toString,
      ReplicationAPIParams.Cookie -> cookie,
      ReplicationAPIParams.Start -> startTimestamp.toString,
      ReplicationAPIParams.Mode -> ReplicationMode.Store.toString)

    // Waiting 50% of the delay
    verifyZeroInteractionsAfter(wait = 400, mockSubscribeAction)
    verifyZeroInteractions(mockUnsubscribeAction)
    verifyOnSessionEndNotCalled()
    sessionManager.sessions should be(List(expectedSession))

    // Try subscribe again
    sessionManager.subscribe(member, txLogProxy, delay = 0, ActionProxy(mockSubscribeAction), ActionProxy(mockUnsubscribeAction),
      ReplicationMode.Bootstrap, onSessionEnd)

    // Wait delay remainder and verify subscribe call is done only once
    Thread.sleep(400)
    verify(mockSubscribeAction, timeout(100)).callOutgoingHandlers(argThat(matchMessage(subscribeRequest)))
    verifyNoMoreInteractions(mockSubscribeAction)
    verifyZeroInteractions(mockUnsubscribeAction)
    verifyOnSessionEndNotCalled()
    sessionManager.sessions should be(List(expectedSession))
  }

  test("session should be activated after receiving a subscribe response") {
    val startTimestamp = Timestamp(1234)
    val endTimestamp = Timestamp(5678)
    val cookie = "miam"
    val sessionId = "9876"
    val expectedSession = ReplicationSession(member, cookie, ReplicationMode.Bootstrap, id = Some(sessionId))

    currentCookie = cookie
    when(txLogProxy.getLastLoggedRecord).thenReturn(Some(Index(-1, Some(startTimestamp))))
    sessionManager.subscribe(member, txLogProxy, delay = 0, ActionProxy(mockSubscribeAction), ActionProxy(mockUnsubscribeAction),
      ReplicationMode.Bootstrap, onSessionEnd)
    val subscribeRequest: Map[String, MValue] = Map(
      ReplicationAPIParams.Token -> token.toString,
      ReplicationAPIParams.Cookie -> cookie,
      ReplicationAPIParams.Start -> startTimestamp.toString,
      ReplicationAPIParams.Mode -> ReplicationMode.Store.toString)

    val subscribeResponse: Map[String, MValue] = Map(
      ReplicationAPIParams.SessionId -> sessionId,
      ReplicationAPIParams.Cookie -> cookie,
      ReplicationAPIParams.Start -> startTimestamp.toString,
      ReplicationAPIParams.End -> endTimestamp.toString)


    val matchCaptor = MessageMatcher(subscribeRequest)
    verify(mockSubscribeAction, timeout(100)).callOutgoingHandlers(argThat(matchCaptor))
    matchCaptor.replyCapturedMessageWith(subscribeResponse)
    verifyNoMoreInteractionsAfter(wait = 100, mockSubscribeAction)
    verifyZeroInteractions(mockUnsubscribeAction)
    verifyOnSessionEndNotCalled()
    sessionManager.sessions should be(List(expectedSession))
  }

  test("session should NOT be activated if receiving wrong cookie in the subscribe response") {
    val startTimestamp = Timestamp(1234)
    val endTimestamp = Timestamp(5678)
    val cookie = "miam"
    val sessionId = "9876"
    val expectedSession = ReplicationSession(member, cookie, ReplicationMode.Bootstrap)

    currentCookie = cookie
    when(txLogProxy.getLastLoggedRecord).thenReturn(Some(Index(-1, Some(startTimestamp))))
    sessionManager.subscribe(member, txLogProxy, delay = 0, ActionProxy(mockSubscribeAction), ActionProxy(mockUnsubscribeAction),
      ReplicationMode.Bootstrap, onSessionEnd)
    val subscribeRequest: Map[String, MValue] = Map(
      ReplicationAPIParams.Token -> token.toString,
      ReplicationAPIParams.Cookie -> cookie,
      ReplicationAPIParams.Start -> startTimestamp.toString,
      ReplicationAPIParams.Mode -> ReplicationMode.Store.toString)

    val subscribeResponse: Map[String, MValue] = Map(
      ReplicationAPIParams.SessionId -> sessionId,
      ReplicationAPIParams.Cookie -> "bad cookie",
      ReplicationAPIParams.Start -> startTimestamp.toString,
      ReplicationAPIParams.End -> endTimestamp.toString)

    val unsubscribeRequest: Map[String, MValue] = Map(
      ReplicationAPIParams.Token -> token.toString,
      ReplicationAPIParams.SubscriptionId -> sessionId,
      ReplicationAPIParams.SessionId -> sessionId)

    val matchCaptor = MessageMatcher(subscribeRequest)
    verify(mockSubscribeAction, timeout(100)).callOutgoingHandlers(argThat(matchCaptor))
    matchCaptor.replyCapturedMessageWith(subscribeResponse)
    verifyNoMoreInteractionsAfter(wait = 100, mockSubscribeAction)
    verify(mockUnsubscribeAction, timeout(100)).callOutgoingHandlers(argThat(matchMessage(unsubscribeRequest)))
    verifyZeroInteractions(mockUnsubscribeAction)
    verifyOnSessionEndNotCalled()
    sessionManager.sessions should be(List(expectedSession))
  }

  test("subscribe response error should remove pending session") {
    val startTimestamp = Timestamp(1234)
    val cookie = "miam"

    currentCookie = cookie
    when(txLogProxy.getLastLoggedRecord).thenReturn(Some(Index(-1, Some(startTimestamp))))
    sessionManager.subscribe(member, txLogProxy, delay = 0, ActionProxy(mockSubscribeAction), ActionProxy(mockUnsubscribeAction),
      ReplicationMode.Bootstrap, onSessionEnd)
    val subscribeRequest: Map[String, MValue] = Map(
      ReplicationAPIParams.Token -> token.toString,
      ReplicationAPIParams.Cookie -> cookie,
      ReplicationAPIParams.Start -> startTimestamp.toString,
      ReplicationAPIParams.Mode -> ReplicationMode.Store.toString)

    val subscribeResponse = new Exception("reponse error")

    // Verify onSessionEnd is invoked with the error
    val matchCaptor = MessageMatcher(subscribeRequest)
    verify(mockSubscribeAction, timeout(100)).callOutgoingHandlers(argThat(matchCaptor))
    matchCaptor.replyCapturedMessageWith(subscribeResponse)
    verifyNoMoreInteractionsAfter(wait = 100, mockSubscribeAction)
    verifyZeroInteractions(mockUnsubscribeAction)
    verifyOnSessionEnd(1, subscribeResponse)
    sessionManager.sessions should be(Nil)
  }

  test("subscribe response with identical start/end timestamps should not active session") {
    val startTimestamp = Timestamp(1234)
    val cookie = "miam"
    val sessionId = "9876"

    currentCookie = cookie
    when(txLogProxy.getLastLoggedRecord).thenReturn(Some(Index(-1, Some(startTimestamp))))
    sessionManager.subscribe(member, txLogProxy, delay = 0, ActionProxy(mockSubscribeAction), ActionProxy(mockUnsubscribeAction),
      ReplicationMode.Bootstrap, onSessionEnd)
    val subscribeRequest: Map[String, MValue] = Map(
      ReplicationAPIParams.Token -> token.toString,
      ReplicationAPIParams.Cookie -> cookie,
      ReplicationAPIParams.Start -> startTimestamp.toString,
      ReplicationAPIParams.Mode -> ReplicationMode.Store.toString)

    val subscribeResponse: Map[String, MValue] = Map(
      ReplicationAPIParams.SessionId -> sessionId,
      ReplicationAPIParams.Cookie -> cookie,
      ReplicationAPIParams.Start -> startTimestamp.toString,
      ReplicationAPIParams.End -> startTimestamp.toString)

    val unsubscribeRequest: Map[String, MValue] = Map(
      ReplicationAPIParams.Token -> token.toString,
      ReplicationAPIParams.SubscriptionId -> sessionId,
      ReplicationAPIParams.SessionId -> sessionId)

    // Verify unsubscribe is sent and onSessionEnd is invoked without an error
    val matchCaptor = MessageMatcher(subscribeRequest)
    verify(mockSubscribeAction, timeout(100)).callOutgoingHandlers(argThat(matchCaptor))
    matchCaptor.replyCapturedMessageWith(subscribeResponse)
    verifyNoMoreInteractionsAfter(wait = 100, mockSubscribeAction)
    verify(mockUnsubscribeAction, timeout(100)).callOutgoingHandlers(argThat(matchMessage(unsubscribeRequest)))
    verifyZeroInteractions(mockUnsubscribeAction)
    verifyOnSessionEnd(1)
    sessionManager.sessions should be(Nil)
  }

  test("subscribe should not invoke master twice if already have a session") {
    val startTimestamp = Timestamp(1234)
    val endTimestamp = Timestamp(5678)
    val cookie = "miam"
    val sessionId = "9876"
    val expectedSession = ReplicationSession(member, cookie, ReplicationMode.Bootstrap, id = Some(sessionId))

    currentCookie = cookie
    when(txLogProxy.getLastLoggedRecord).thenReturn(Some(Index(-1, Some(startTimestamp))))
    sessionManager.subscribe(member, txLogProxy, delay = 0, ActionProxy(mockSubscribeAction), ActionProxy(mockUnsubscribeAction),
      ReplicationMode.Bootstrap, onSessionEnd)
    val subscribeRequest: Map[String, MValue] = Map(
      ReplicationAPIParams.Token -> token.toString,
      ReplicationAPIParams.Cookie -> cookie,
      ReplicationAPIParams.Start -> startTimestamp.toString,
      ReplicationAPIParams.Mode -> ReplicationMode.Store.toString)

    val subscribeResponse: Map[String, MValue] = Map(
      ReplicationAPIParams.SessionId -> sessionId,
      ReplicationAPIParams.Cookie -> cookie,
      ReplicationAPIParams.Start -> startTimestamp.toString,
      ReplicationAPIParams.End -> endTimestamp.toString)

    val matchCaptor = MessageMatcher(subscribeRequest)
    verify(mockSubscribeAction, timeout(100)).callOutgoingHandlers(argThat(matchCaptor))
    matchCaptor.replyCapturedMessageWith(subscribeResponse)
    verifyNoMoreInteractionsAfter(wait = 100, mockSubscribeAction)
    verifyZeroInteractions(mockUnsubscribeAction)
    verifyOnSessionEndNotCalled()
    sessionManager.sessions should be(List(expectedSession))

    sessionManager.subscribe(member, txLogProxy, delay = 0, ActionProxy(mockSubscribeAction), ActionProxy(mockUnsubscribeAction),
      ReplicationMode.Bootstrap, onSessionEnd)
    verifyZeroInteractionsAfter(wait = 100, mockSubscribeAction, mockUnsubscribeAction)
    verifyOnSessionEndNotCalled()
    sessionManager.sessions should be(List(expectedSession))
  }

  test("subscribe when have another service member session pending") {
    val startTimestamp = Timestamp(1234)
    val cookie = "miam"
    val expectedSession = ReplicationSession(member, cookie, ReplicationMode.Bootstrap)

    // First session with a different service member
    val otherMember = ResolvedServiceMember(service.name, token + 1000, Seq(TokenRange.All))
    val otherCookie = "other"
    val otherSession = ReplicationSession(otherMember, otherCookie, ReplicationMode.Bootstrap)
    currentCookie = otherCookie
    when(txLogProxy.getLastLoggedRecord).thenReturn(Some(Index(-1, Some(startTimestamp))))
    sessionManager.subscribe(otherMember, txLogProxy, delay = 5000, ActionProxy(mockSubscribeAction), ActionProxy(mockUnsubscribeAction),
      ReplicationMode.Bootstrap, onSessionEnd)
    sessionManager.sessions should be(List(otherSession))
    verifyZeroInteractions(mockSubscribeAction, mockUnsubscribeAction)

    // Open new replication session
    currentCookie = cookie
    when(txLogProxy.getLastLoggedRecord).thenReturn(Some(Index(-1, Some(startTimestamp))))
    sessionManager.subscribe(member, txLogProxy, delay = 0, ActionProxy(mockSubscribeAction), ActionProxy(mockUnsubscribeAction),
      ReplicationMode.Bootstrap, onSessionEnd)
    val subscribeRequest: Map[String, MValue] = Map(
      ReplicationAPIParams.Token -> token.toString,
      ReplicationAPIParams.Cookie -> cookie,
      ReplicationAPIParams.Start -> startTimestamp.toString,
      ReplicationAPIParams.Mode -> ReplicationMode.Store.toString)

    // Validate now have two pending sessions
    verify(mockSubscribeAction, timeout(100)).callOutgoingHandlers(argThat(matchMessage(subscribeRequest)))
    verifyNoMoreInteractions(mockSubscribeAction)
    verifyZeroInteractions(mockUnsubscribeAction)
    verifyOnSessionEndNotCalled()
    sessionManager.sessions.toSet should be(Set(otherSession, expectedSession))
  }

  test("unsubscribe before delay expires should should not invoke master") {
    val startTimestamp = Timestamp(1234)
    val cookie = "miam"

    currentCookie = cookie
    when(txLogProxy.getLastLoggedRecord).thenReturn(Some(Index(-1, Some(startTimestamp))))
    sessionManager.subscribe(member, txLogProxy, delay = 500, ActionProxy(mockSubscribeAction), ActionProxy(mockUnsubscribeAction),
      ReplicationMode.Bootstrap, onSessionEnd)

    // Ensure no interaction yet
    verifyZeroInteractionsAfter(wait = 200, mockSubscribeAction, mockUnsubscribeAction)
    verifyOnSessionEndNotCalled()
    sessionManager.sessions should be(List(ReplicationSession(member, cookie, ReplicationMode.Bootstrap)))

    // Unsubscribe and wait end of the delay
    sessionManager.unsubscribe(member)
    verifyZeroInteractionsAfter(wait = 400, mockSubscribeAction, mockUnsubscribeAction)
    verifyOnSessionEndNotCalled()
    sessionManager.sessions should be(Nil)
  }

  test("unsubscribe pending session should ignore subscribe response") {
    val startTimestamp = Timestamp(1234)
    val cookie = "miam"
    val sessionId = "9876"
    val expectedSession = ReplicationSession(member, cookie, ReplicationMode.Bootstrap)

    currentCookie = cookie
    when(txLogProxy.getLastLoggedRecord).thenReturn(Some(Index(-1, Some(startTimestamp))))
    sessionManager.subscribe(member, txLogProxy, delay = 0, ActionProxy(mockSubscribeAction), ActionProxy(mockUnsubscribeAction),
      ReplicationMode.Bootstrap, onSessionEnd)

    val subscribeRequest: Map[String, MValue] = Map(
      ReplicationAPIParams.Token -> token.toString,
      ReplicationAPIParams.Cookie -> cookie,
      ReplicationAPIParams.Start -> startTimestamp.toString,
      ReplicationAPIParams.Mode -> ReplicationMode.Store.toString)

    val subscribeResponse: Map[String, MValue] = Map(
      ReplicationAPIParams.SessionId -> sessionId,
      ReplicationAPIParams.Cookie -> cookie,
      ReplicationAPIParams.Start -> startTimestamp.toString,
      ReplicationAPIParams.End -> "5678")

    val unsubscribeRequest: Map[String, MValue] = Map(
      ReplicationAPIParams.Token -> token.toString,
      ReplicationAPIParams.SubscriptionId -> sessionId,
      ReplicationAPIParams.SessionId -> sessionId)

    // Verify session is pending (open session reponse is delayed after unsubscribe)
    val matchCaptor = MessageMatcher(subscribeRequest)
    verify(mockSubscribeAction, timeout(100)).callOutgoingHandlers(argThat(matchCaptor))
    verifyNoMoreInteractionsAfter(wait = 100, mockSubscribeAction)
    verifyZeroInteractions(mockUnsubscribeAction)
    verifyOnSessionEndNotCalled()
    sessionManager.sessions should be(List(expectedSession))

    // Unsubscribe pending session before receiving response
    sessionManager.unsubscribe(member)
    verifyZeroInteractionsAfter(wait = 100, mockSubscribeAction)
    verifyZeroInteractions(mockUnsubscribeAction)
    verifyOnSessionEndNotCalled()
    sessionManager.sessions should be(Nil)

    // Finally receive the subscribe response (after unsubscribing)
    matchCaptor.replyCapturedMessageWith(subscribeResponse)

    // Verify unsubscribe is sent after getting the late subscribe response
    verify(mockUnsubscribeAction, timeout(100)).callOutgoingHandlers(argThat(matchMessage(unsubscribeRequest)))
    verifyZeroInteractionsAfter(wait = 100, mockSubscribeAction, mockUnsubscribeAction)
    verifyOnSessionEndNotCalled()
    sessionManager.sessions should be(Nil)
  }

  test("unsubscribe should unsubscribe proper session") {
    val startTimestamp = Timestamp(1234)
    val endTimestamp = Timestamp(5678)
    val cookie = "miam"
    val sessionId = "9876"
    val expectedSession = ReplicationSession(member, cookie, ReplicationMode.Bootstrap, id = Some(sessionId))

    currentCookie = cookie
    when(txLogProxy.getLastLoggedRecord).thenReturn(Some(Index(-1, Some(startTimestamp))))
    sessionManager.subscribe(member, txLogProxy, delay = 0, ActionProxy(mockSubscribeAction), ActionProxy(mockUnsubscribeAction),
      ReplicationMode.Bootstrap, onSessionEnd)
    val subscribeRequest: Map[String, MValue] = Map(
      ReplicationAPIParams.Token -> token.toString,
      ReplicationAPIParams.Cookie -> cookie,
      ReplicationAPIParams.Start -> startTimestamp.toString,
      ReplicationAPIParams.Mode -> ReplicationMode.Store.toString)

    val subscribeResponse: Map[String, MValue] = Map(
      ReplicationAPIParams.SessionId -> sessionId,
      ReplicationAPIParams.Cookie -> cookie,
      ReplicationAPIParams.Start -> startTimestamp.toString,
      ReplicationAPIParams.End -> endTimestamp.toString)

    // Verify session is activated
    val matchCaptor = MessageMatcher(subscribeRequest)
    verify(mockSubscribeAction, timeout(100)).callOutgoingHandlers(argThat(matchCaptor))
    matchCaptor.replyCapturedMessageWith(subscribeResponse)
    verifyNoMoreInteractionsAfter(wait = 100, mockSubscribeAction)
    verifyZeroInteractions(mockUnsubscribeAction)
    verifyOnSessionEndNotCalled()
    sessionManager.sessions should be(List(expectedSession))

    // Unsubscribe member without session (should do nothing)
    val otherMember = ResolvedServiceMember(service.name, token + 1000, Seq(TokenRange.All))
    sessionManager.unsubscribe(otherMember)
    verifyZeroInteractionsAfter(wait = 100, mockUnsubscribeAction)
    verifyZeroInteractions(mockSubscribeAction)
    sessionManager.sessions should be(List(expectedSession))

    // Unsubscribe
    val unsubscribeRequest: Map[String, MValue] = Map(
      ReplicationAPIParams.Token -> token.toString,
      ReplicationAPIParams.SubscriptionId -> sessionId,
      ReplicationAPIParams.SessionId -> sessionId)

    sessionManager.unsubscribe(member)
    verify(mockUnsubscribeAction, timeout(100)).callOutgoingHandlers(argThat(matchMessage(unsubscribeRequest)))
    verifyZeroInteractionsAfter(wait = 100, mockUnsubscribeAction, mockSubscribeAction)
    sessionManager.sessions should be(Nil)
  }

  test("replication mesage should store in tx log and store in proper order") {
    val startTimestamp = 0L
    val session = subscribe(startTimestamp)

    val tx1 = createRequestMessage(timestamp = 100L)
    val tx2 = createRequestMessage(timestamp = 200L)
    val tx3 = createRequestMessage(timestamp = 300L)

    val ordered = inOrder(txLogProxy.mockAppender, mockStore)
    currentLogId = 1000

    // Publish transactions out of order
    sessionManager.handleReplicationMessage(createTransactionMessage(sequence = 2, session.id.get, tx2))
    sessionManager.handleReplicationMessage(createTransactionMessage(sequence = 3, session.id.get, tx3))
    sessionManager.handleReplicationMessage(createTransactionMessage(sequence = 1, session.id.get, tx1))

    // Wait for transaction processing, unfortunatly timeout mode is not implemented with InOrder
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
    verifyZeroInteractions(mockSubscribeAction, mockUnsubscribeAction)
  }

  test("replication message tx log append error should result in consistency error") {
    val startTimestamp = 0L
    val session = subscribe(startTimestamp)

    currentLogId = 1000

    val tx1 = createRequestMessage(timestamp = 100L)
    val tx2 = createRequestMessage(timestamp = 200L)
    val tx3 = createRequestMessage(timestamp = 300L)

    val ordered = inOrder(txLogProxy.mockAppender, mockStore)
    when(txLogProxy.mockAppender.append(LogRecord(1002, tx1.timestamp, tx2))).thenThrow(new RuntimeException())

    // Publish transactions out of order
    sessionManager.handleReplicationMessage(createTransactionMessage(sequence = 2, session.id.get, tx2))
    sessionManager.handleReplicationMessage(createTransactionMessage(sequence = 1, session.id.get, tx1))
    sessionManager.handleReplicationMessage(createTransactionMessage(sequence = 3, session.id.get, tx3))

    // Wait for transaction processing, unfortunatly timeout mode is not implemented with InOrder
    Thread.sleep(100)

    // Verify transactions applied in order up to the error
    ordered.verify(txLogProxy.mockAppender).append(LogRecord(1000, Some(startTimestamp), tx1))
    ordered.verify(mockStore).writeTransaction(argThat(matchMessage(tx1)))
    ordered.verify(txLogProxy.mockAppender).append(LogRecord(1001, Some(startTimestamp), createResponseMessage(tx1)))
    ordered.verify(txLogProxy.mockAppender).append(LogRecord(1002, tx1.timestamp, tx2))
    verifyNoMoreInteractions(txLogProxy.mockAppender, mockStore)
    pushTxReplyCount should be(1)

    val unsubscribeRequest: Map[String, MValue] = Map(
      ReplicationAPIParams.Token -> token.toString,
      ReplicationAPIParams.SubscriptionId -> session.id.get,
      ReplicationAPIParams.SessionId -> session.id.get)
    verify(mockUnsubscribeAction, timeout(100)).callOutgoingHandlers(argThat(matchMessage(unsubscribeRequest)))
    verifyZeroInteractions(mockSubscribeAction)
  }

  test("replication message consistent store write error should result in consistency error") {
    val startTimestamp = 0L
    val session = subscribe(startTimestamp)

    currentLogId = 1000

    val tx1 = createRequestMessage(timestamp = 100L)
    val tx2 = createRequestMessage(timestamp = 200L)

    val ordered = inOrder(txLogProxy.mockAppender, mockStore)
    when(mockStore.writeTransaction(argThat(matchMessage(tx1)))).thenThrow(new RuntimeException())

    // Publish transactions out of order
    sessionManager.handleReplicationMessage(createTransactionMessage(sequence = 2, session.id.get, tx2))
    sessionManager.handleReplicationMessage(createTransactionMessage(sequence = 1, session.id.get, tx1))

    // Wait for transaction processing, unfortunatly timeout mode is not implemented with InOrder
    Thread.sleep(100)

    // Verify transactions applied in order up to the error
    ordered.verify(txLogProxy.mockAppender).append(LogRecord(1000, Some(startTimestamp), tx1))
    ordered.verify(mockStore).writeTransaction(argThat(matchMessage(tx1)))
    verifyNoMoreInteractions(txLogProxy.mockAppender, mockStore)
    pushTxReplyCount should be(0)

    val unsubscribeRequest: Map[String, MValue] = Map(
      ReplicationAPIParams.Token -> token.toString,
      ReplicationAPIParams.SubscriptionId -> session.id.get,
      ReplicationAPIParams.SessionId -> session.id.get)
    verify(mockUnsubscribeAction, timeout(100)).callOutgoingHandlers(argThat(matchMessage(unsubscribeRequest)))
    verifyZeroInteractions(mockSubscribeAction)
  }

  test("skip replication message sequence # should result in consistency error") {
    val startTimestamp = 0L
    val session = subscribe(startTimestamp)

    currentLogId = 1000

    val tx1 = createRequestMessage(timestamp = 100L)
    val tx2 = createRequestMessage(timestamp = 200L)
    val tx3 = createRequestMessage(timestamp = 300L)

    val ordered = inOrder(txLogProxy.mockAppender, mockStore)

    // Publish transactions with a sequence # gap, untill timeout and resume
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

    val unsubscribeRequest: Map[String, MValue] = Map(
      ReplicationAPIParams.Token -> token.toString,
      ReplicationAPIParams.SubscriptionId -> session.id.get,
      ReplicationAPIParams.SessionId -> session.id.get)
    verify(mockUnsubscribeAction, timeout(100)).callOutgoingHandlers(argThat(matchMessage(unsubscribeRequest)))
    verifyZeroInteractions(mockSubscribeAction)
  }

  test("stop receiving replication message should result in timeout error") {
    val startTimestamp = 0L
    val session = subscribe(startTimestamp)

    currentLogId = 1000

    val tx1 = createRequestMessage(timestamp = 100L)

    val ordered = inOrder(txLogProxy.mockAppender, mockStore)

    // Publish transactions with a sequence # gap, untill timeout and resume
    sessionManager.handleReplicationMessage(createTransactionMessage(sequence = 1, session.id.get, tx1))
    Thread.sleep((sessionTimeout * 1.5).toLong)

    // Verify transactions applied in order up to the sequence # gap
    ordered.verify(txLogProxy.mockAppender).append(LogRecord(1000, Some(startTimestamp), tx1))
    ordered.verify(mockStore).writeTransaction(argThat(matchMessage(tx1)))
    ordered.verify(txLogProxy.mockAppender).append(LogRecord(1001, Some(startTimestamp), createResponseMessage(tx1)))
    verifyNoMoreInteractions(txLogProxy.mockAppender, mockStore)
    pushTxReplyCount should be(1)

    val unsubscribeRequest: Map[String, MValue] = Map(
      ReplicationAPIParams.Token -> token.toString,
      ReplicationAPIParams.SubscriptionId -> session.id.get,
      ReplicationAPIParams.SessionId -> session.id.get)
    verify(mockUnsubscribeAction, timeout(100)).callOutgoingHandlers(argThat(matchMessage(unsubscribeRequest)))
    verifyZeroInteractions(mockSubscribeAction)
  }

  test("idle message should not result in timeout error") {
    val startTimestamp = 0L
    val session = subscribe(startTimestamp)
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

    verifyZeroInteractions(txLogProxy.mockAppender, mockStore, mockSubscribeAction, mockUnsubscribeAction)
    pushTxReplyCount should be(3)
  }
}