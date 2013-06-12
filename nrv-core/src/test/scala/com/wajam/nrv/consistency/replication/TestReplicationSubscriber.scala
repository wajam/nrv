package com.wajam.nrv.consistency.replication

import org.scalatest.BeforeAndAfter
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._
import org.scalatest.matchers.ShouldMatchers._
import org.mockito.Matchers._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import com.wajam.nrv.service._
import com.wajam.nrv.consistency.{TestTransactionBase, TransactionLogProxy, ResolvedServiceMember, ConsistentStore}
import com.wajam.nrv.data._
import com.wajam.nrv.utils.timestamp.Timestamp
import MessageMatcher._
import com.wajam.nrv.consistency.persistence.LogRecord.Index
import java.util.UUID
import com.wajam.nrv.utils.IdGenerator
import com.wajam.nrv.consistency.persistence.LogRecord

@RunWith(classOf[JUnitRunner])
class TestReplicationSubscriber extends TestTransactionBase with BeforeAndAfter with MockitoSugar {

  var service: Service = null
  var member: ResolvedServiceMember = null
  var txLogProxy: TransactionLogProxy = null
  var mockStore: ConsistentStore = null
  var subscribeAction: ActionProxy = null
  var unsubscribeAction: ActionProxy = null

  var currentCookie: String = null
  var currentLogId: Long = 0
  var subscriber: ReplicationSubscriber = null

  val subscriptionTimeout = 1500L
  val token = 0
  var subscriptionEndErrors: List[Exception] = Nil
  var subscriptionEndCalls = 0
  var publishReplyCount = 0

  before {
    service = new Service("service")
    member = ResolvedServiceMember(service.name, token, Seq(TokenRange.All))
    txLogProxy = new TransactionLogProxy

    mockStore = mock[ConsistentStore]
    subscribeAction = ActionProxy()
    unsubscribeAction = ActionProxy()

    subscriber = new ReplicationSubscriber(service, mockStore, subscriptionTimeout, commitFrequency = 0,
      logIdGenerator = new IdGenerator[Long] {
        def nextId = {
          val id = currentLogId
          currentLogId += 1
          id
        }
      }) {
      override def nextId = currentCookie
    }
    subscriber.start()
  }

  after {
    subscriber.stop()
    subscriber = null
    unsubscribeAction = null
    subscribeAction = null
    mockStore = null
    txLogProxy = null

    subscriptionEndErrors = Nil
    subscriptionEndCalls = 0
    publishReplyCount = 0
  }

  def onSubscriptionEnd(exception: Option[Exception]) {
    subscriptionEndCalls += 1
    exception match {
      case Some(e) => subscriptionEndErrors = e :: subscriptionEndErrors
      case _ =>
    }
  }

  def verifyOnSubscriptionEnd(expectedCalls: Int, expectedErrors: Exception*) {
    subscriptionEndCalls should be(expectedCalls)
    subscriptionEndErrors should be(expectedErrors)
  }

  def verifyOnSubscriptionEndNotCalled() {
    verifyOnSubscriptionEnd(expectedCalls = 0)
  }

  def subscribe(startTimestamp: Timestamp = 0L, cookie: String = UUID.randomUUID().toString): ReplicationSubscription = {
    val endTimestamp = Timestamp(5678)
    val subId = "9876"

    currentCookie = cookie
    when(txLogProxy.getLastLoggedRecord).thenReturn(Some(Index(-1, Some(startTimestamp))))
    subscriber.subscribe(member, txLogProxy, delay = 0, subscribeAction, unsubscribeAction,
      ReplicationMode.Store, onSubscriptionEnd)
    val subscribeRequest: Map[String, MValue] = Map(
      ReplicationParam.Token -> token.toString,
      ReplicationParam.Cookie -> cookie,
      ReplicationParam.Start -> startTimestamp.toString,
      ReplicationParam.Mode -> ReplicationMode.Store.toString)

    val subscribeResponse: Map[String, MValue] = Map(
      ReplicationParam.SubscriptionId -> subId,
      ReplicationParam.Cookie -> cookie,
      ReplicationParam.Start -> startTimestamp.toString,
      ReplicationParam.End -> endTimestamp.toString)

    // Verify subscription is activated
    val matchCaptor = MessageMatcher(subscribeRequest)
    verify(subscribeAction.mockAction, timeout(100)).callOutgoingHandlers(argThat(matchCaptor))
    matchCaptor.replyCapturedMessageWith(subscribeResponse)
    subscribeAction.verifyNoMoreInteractions(wait = 100)
    unsubscribeAction.verifyZeroInteractions()

    subscriber.subscriptions.find(_.cookie == cookie).get
  }

  def createPublishMessage(sequence: Long, subId: String, transaction: Message): InMessage = {
    val params: Map[String, MValue] = Map(
      ReplicationParam.Sequence -> sequence.toString,
      ReplicationParam.SubscriptionId -> subId,
      ReplicationParam.Timestamp -> transaction.timestamp.get.toString)
    val publishMessage = new InMessage(params, data = transaction)
    publishMessage.replyCallback = (_) => publishReplyCount += 1
    publishMessage
  }

  def createKeepAliveMessage(sequence: Long, subId: String): InMessage = {
    val params: Map[String, MValue] = Map(
      ReplicationParam.Sequence -> sequence.toString,
      ReplicationParam.SubscriptionId -> subId)
    val publishMessage = new InMessage(params)
    publishMessage.replyCallback = (_) => publishReplyCount += 1
    publishMessage
  }

  test("subscribe should invoke master publisher") {
    val startTimestamp = Timestamp(1234)
    val cookie = "miam"

    currentCookie = cookie
    when(txLogProxy.getLastLoggedRecord).thenReturn(Some(Index(-1, Some(startTimestamp))))
    subscriber.subscribe(member, txLogProxy, delay = 0, subscribeAction, unsubscribeAction,
      ReplicationMode.Store, onSubscriptionEnd)
    val subscribeRequest: Map[String, MValue] = Map(
      ReplicationParam.Token -> token.toString,
      ReplicationParam.Cookie -> cookie,
      ReplicationParam.Start -> startTimestamp.toString,
      ReplicationParam.Mode -> ReplicationMode.Store.toString)

    verify(subscribeAction.mockAction, timeout(100)).callOutgoingHandlers(argThat(matchMessage(subscribeRequest)))
    subscribeAction.verifyNoMoreInteractions()
    unsubscribeAction.verifyZeroInteractions()
    verifyOnSubscriptionEndNotCalled()
    subscriber.subscriptions should be(List(ReplicationSubscription(member, cookie, ReplicationMode.Store)))
  }

  test("subscribe should invoke master publisher only after delay") {
    val startTimestamp = Timestamp(1234)
    val cookie = "miam"

    currentCookie = cookie
    when(txLogProxy.getLastLoggedRecord).thenReturn(Some(Index(-1, Some(startTimestamp))))
    subscriber.subscribe(member, txLogProxy, delay = 800, subscribeAction, unsubscribeAction,
      ReplicationMode.Store, onSubscriptionEnd)
    val subscribeRequest: Map[String, MValue] = Map(
      ReplicationParam.Token -> token.toString,
      ReplicationParam.Cookie -> cookie,
      ReplicationParam.Start -> startTimestamp.toString,
      ReplicationParam.Mode -> ReplicationMode.Store.toString)

    // Wait 75% of the delay and ensure no interaction yet
    subscribeAction.verifyZeroInteractions(wait = 600)
    unsubscribeAction.verifyZeroInteractions()
    verifyOnSubscriptionEndNotCalled()
    subscriber.subscriptions should be(List(ReplicationSubscription(member, cookie, ReplicationMode.Store)))

    // Wait delay remainder and verify subscribe call is done
    verify(subscribeAction.mockAction, timeout(200)).callOutgoingHandlers(argThat(matchMessage(subscribeRequest)))
    subscribeAction.verifyNoMoreInteractions()
    unsubscribeAction.verifyZeroInteractions()
    verifyOnSubscriptionEndNotCalled()
    subscriber.subscriptions should be(List(ReplicationSubscription(member, cookie, ReplicationMode.Store)))
  }

  test("subscribe should not invoke publisher twice if a subscription is already pending") {
    val startTimestamp = Timestamp(1234)
    val cookie = "miam"
    val expectedSubscription = ReplicationSubscription(member, cookie, ReplicationMode.Store)

    currentCookie = cookie
    when(txLogProxy.getLastLoggedRecord).thenReturn(Some(Index(-1, Some(startTimestamp))))
    subscriber.subscribe(member, txLogProxy, delay = 800, subscribeAction, unsubscribeAction,
      ReplicationMode.Store, onSubscriptionEnd)
    val subscribeRequest: Map[String, MValue] = Map(
      ReplicationParam.Token -> token.toString,
      ReplicationParam.Cookie -> cookie,
      ReplicationParam.Start -> startTimestamp.toString,
      ReplicationParam.Mode -> ReplicationMode.Store.toString)

    // Waiting 50% of the delay
    subscribeAction.verifyZeroInteractions(wait = 400)
    unsubscribeAction.verifyZeroInteractions()
    verifyOnSubscriptionEndNotCalled()
    subscriber.subscriptions should be(List(expectedSubscription))

    // Try subscribe again
    subscriber.subscribe(member, txLogProxy, delay = 0, subscribeAction, unsubscribeAction,
      ReplicationMode.Store, onSubscriptionEnd)

    // Wait delay remainder and verify subscribe call is done only once
    Thread.sleep(400)
    verify(subscribeAction.mockAction, timeout(100)).callOutgoingHandlers(argThat(matchMessage(subscribeRequest)))
    subscribeAction.verifyNoMoreInteractions()
    unsubscribeAction.verifyZeroInteractions()
    verifyOnSubscriptionEndNotCalled()
    subscriber.subscriptions should be(List(expectedSubscription))
  }

  test("subscription should be activated after receiving a subscribe response") {
    val startTimestamp = Timestamp(1234)
    val endTimestamp = Timestamp(5678)
    val cookie = "miam"
    val subId = "9876"
    val expectedSubscription = ReplicationSubscription(member, cookie, ReplicationMode.Store, id = Some(subId))

    currentCookie = cookie
    when(txLogProxy.getLastLoggedRecord).thenReturn(Some(Index(-1, Some(startTimestamp))))
    subscriber.subscribe(member, txLogProxy, delay = 0, subscribeAction, unsubscribeAction,
      ReplicationMode.Store, onSubscriptionEnd)
    val subscribeRequest: Map[String, MValue] = Map(
      ReplicationParam.Token -> token.toString,
      ReplicationParam.Cookie -> cookie,
      ReplicationParam.Start -> startTimestamp.toString,
      ReplicationParam.Mode -> ReplicationMode.Store.toString)

    val subscribeResponse: Map[String, MValue] = Map(
      ReplicationParam.SubscriptionId -> subId,
      ReplicationParam.Cookie -> cookie,
      ReplicationParam.Start -> startTimestamp.toString,
      ReplicationParam.End -> endTimestamp.toString)


    val matchCaptor = MessageMatcher(subscribeRequest)
    verify(subscribeAction.mockAction, timeout(100)).callOutgoingHandlers(argThat(matchCaptor))
    matchCaptor.replyCapturedMessageWith(subscribeResponse)
    subscribeAction.verifyNoMoreInteractions(wait = 100)
    unsubscribeAction.verifyZeroInteractions()
    verifyOnSubscriptionEndNotCalled()
    subscriber.subscriptions should be(List(expectedSubscription))
  }

  test("subscription should NOT be activated if receiving wrong cookie in the subscribe response") {
    val startTimestamp = Timestamp(1234)
    val endTimestamp = Timestamp(5678)
    val cookie = "miam"
    val subId = "9876"
    val expectedSubscription = ReplicationSubscription(member, cookie, ReplicationMode.Store)

    currentCookie = cookie
    when(txLogProxy.getLastLoggedRecord).thenReturn(Some(Index(-1, Some(startTimestamp))))
    subscriber.subscribe(member, txLogProxy, delay = 0, subscribeAction, unsubscribeAction,
      ReplicationMode.Store, onSubscriptionEnd)
    val subscribeRequest: Map[String, MValue] = Map(
      ReplicationParam.Token -> token.toString,
      ReplicationParam.Cookie -> cookie,
      ReplicationParam.Start -> startTimestamp.toString,
      ReplicationParam.Mode -> ReplicationMode.Store.toString)

    val subscribeResponse: Map[String, MValue] = Map(
      ReplicationParam.SubscriptionId -> subId,
      ReplicationParam.Cookie -> "bad cookie",
      ReplicationParam.Start -> startTimestamp.toString,
      ReplicationParam.End -> endTimestamp.toString)

    val unsubscribeRequest: Map[String, MValue] = Map(
      ReplicationParam.Token -> token.toString,
      ReplicationParam.SubscriptionId -> subId)

    val matchCaptor = MessageMatcher(subscribeRequest)
    verify(subscribeAction.mockAction, timeout(100)).callOutgoingHandlers(argThat(matchCaptor))
    matchCaptor.replyCapturedMessageWith(subscribeResponse)
    subscribeAction.verifyNoMoreInteractions(wait = 100)
    verify(unsubscribeAction.mockAction, timeout(100)).callOutgoingHandlers(argThat(matchMessage(unsubscribeRequest)))
    unsubscribeAction.verifyNoMoreInteractions()
    verifyOnSubscriptionEndNotCalled()
    subscriber.subscriptions should be(List(expectedSubscription))
  }

  test("subscribe response error should remove pending subscription") {
    val startTimestamp = Timestamp(1234)
    val cookie = "miam"

    currentCookie = cookie
    when(txLogProxy.getLastLoggedRecord).thenReturn(Some(Index(-1, Some(startTimestamp))))
    subscriber.subscribe(member, txLogProxy, delay = 0, subscribeAction, unsubscribeAction,
      ReplicationMode.Store, onSubscriptionEnd)
    val subscribeRequest: Map[String, MValue] = Map(
      ReplicationParam.Token -> token.toString,
      ReplicationParam.Cookie -> cookie,
      ReplicationParam.Start -> startTimestamp.toString,
      ReplicationParam.Mode -> ReplicationMode.Store.toString)

    val subscribeResponse = new Exception("reponse error")

    // Verify onSubscriptionEnd is invoked with the error
    val matchCaptor = MessageMatcher(subscribeRequest)
    verify(subscribeAction.mockAction, timeout(100)).callOutgoingHandlers(argThat(matchCaptor))
    matchCaptor.replyCapturedMessageWith(subscribeResponse)
    subscribeAction.verifyNoMoreInteractions(wait = 100)
    unsubscribeAction.verifyZeroInteractions()
    verifyOnSubscriptionEnd(1, subscribeResponse)
    subscriber.subscriptions should be(Nil)
  }

  test("subscribe response with identical start/end timestamps should not active subscription") {
    val startTimestamp = Timestamp(1234)
    val cookie = "miam"
    val subId = "9876"

    currentCookie = cookie
    when(txLogProxy.getLastLoggedRecord).thenReturn(Some(Index(-1, Some(startTimestamp))))
    subscriber.subscribe(member, txLogProxy, delay = 0, subscribeAction, unsubscribeAction,
      ReplicationMode.Store, onSubscriptionEnd)
    val subscribeRequest: Map[String, MValue] = Map(
      ReplicationParam.Token -> token.toString,
      ReplicationParam.Cookie -> cookie,
      ReplicationParam.Start -> startTimestamp.toString,
      ReplicationParam.Mode -> ReplicationMode.Store.toString)

    val subscribeResponse: Map[String, MValue] = Map(
      ReplicationParam.SubscriptionId -> subId,
      ReplicationParam.Cookie -> cookie,
      ReplicationParam.Start -> startTimestamp.toString,
      ReplicationParam.End -> startTimestamp.toString)

    val unsubscribeRequest: Map[String, MValue] = Map(
      ReplicationParam.Token -> token.toString,
      ReplicationParam.SubscriptionId -> subId)

    // Verify unsubscribe is sent and onSubscriptionEnd is invoked without an error
    val matchCaptor = MessageMatcher(subscribeRequest)
    verify(subscribeAction.mockAction, timeout(100)).callOutgoingHandlers(argThat(matchCaptor))
    matchCaptor.replyCapturedMessageWith(subscribeResponse)
    subscribeAction.verifyNoMoreInteractions(wait = 100)
    verify(unsubscribeAction.mockAction, timeout(100)).callOutgoingHandlers(argThat(matchMessage(unsubscribeRequest)))
    unsubscribeAction.verifyNoMoreInteractions()
    verifyOnSubscriptionEnd(1)
    subscriber.subscriptions should be(Nil)
  }

  test("subscribe should not invoke publisher twice if already have a subscription") {
    val startTimestamp = Timestamp(1234)
    val endTimestamp = Timestamp(5678)
    val cookie = "miam"
    val subId = "9876"
    val expectedSubscription = ReplicationSubscription(member, cookie, ReplicationMode.Store, id = Some(subId))

    currentCookie = cookie
    when(txLogProxy.getLastLoggedRecord).thenReturn(Some(Index(-1, Some(startTimestamp))))
    subscriber.subscribe(member, txLogProxy, delay = 0, subscribeAction, unsubscribeAction,
      ReplicationMode.Store, onSubscriptionEnd)
    val subscribeRequest: Map[String, MValue] = Map(
      ReplicationParam.Token -> token.toString,
      ReplicationParam.Cookie -> cookie,
      ReplicationParam.Start -> startTimestamp.toString,
      ReplicationParam.Mode -> ReplicationMode.Store.toString)

    val subscribeResponse: Map[String, MValue] = Map(
      ReplicationParam.SubscriptionId -> subId,
      ReplicationParam.Cookie -> cookie,
      ReplicationParam.Start -> startTimestamp.toString,
      ReplicationParam.End -> endTimestamp.toString)

    val matchCaptor = MessageMatcher(subscribeRequest)
    verify(subscribeAction.mockAction, timeout(100)).callOutgoingHandlers(argThat(matchCaptor))
    matchCaptor.replyCapturedMessageWith(subscribeResponse)
    subscribeAction.verifyNoMoreInteractions(wait = 100)
    unsubscribeAction.verifyZeroInteractions()
    verifyOnSubscriptionEndNotCalled()
    subscriber.subscriptions should be(List(expectedSubscription))

    subscriber.subscribe(member, txLogProxy, delay = 0, subscribeAction, unsubscribeAction,
      ReplicationMode.Store, onSubscriptionEnd)
    subscribeAction.verifyZeroInteractions(wait = 100)
    unsubscribeAction.verifyZeroInteractions()
    verifyOnSubscriptionEndNotCalled()
    subscriber.subscriptions should be(List(expectedSubscription))
  }

  test("subscribe when have another service member subscription pending") {
    val startTimestamp = Timestamp(1234)
    val cookie = "miam"
    val expectedSubscription = ReplicationSubscription(member, cookie, ReplicationMode.Store)

    // Subscription first with a different service member
    val otherMember = ResolvedServiceMember(service.name, token + 1000, Seq(TokenRange.All))
    val otherCookie = "other"
    val otherSubscription = ReplicationSubscription(otherMember, otherCookie, ReplicationMode.Store)
    currentCookie = otherCookie
    when(txLogProxy.getLastLoggedRecord).thenReturn(Some(Index(-1, Some(startTimestamp))))
    subscriber.subscribe(otherMember, txLogProxy, delay = 5000, subscribeAction, unsubscribeAction,
      ReplicationMode.Store, onSubscriptionEnd)
    subscriber.subscriptions should be(List(otherSubscription))
    subscribeAction.verifyZeroInteractions()
    unsubscribeAction.verifyZeroInteractions()

    // Perform subscription
    currentCookie = cookie
    when(txLogProxy.getLastLoggedRecord).thenReturn(Some(Index(-1, Some(startTimestamp))))
    subscriber.subscribe(member, txLogProxy, delay = 0, subscribeAction, unsubscribeAction,
      ReplicationMode.Store, onSubscriptionEnd)
    val subscribeRequest: Map[String, MValue] = Map(
      ReplicationParam.Token -> token.toString,
      ReplicationParam.Cookie -> cookie,
      ReplicationParam.Start -> startTimestamp.toString,
      ReplicationParam.Mode -> ReplicationMode.Store.toString)

    // Validate now have two pending subscription
    verify(subscribeAction.mockAction, timeout(100)).callOutgoingHandlers(argThat(matchMessage(subscribeRequest)))
    subscribeAction.verifyNoMoreInteractions()
    unsubscribeAction.verifyZeroInteractions()
    verifyOnSubscriptionEndNotCalled()
    subscriber.subscriptions.toSet should be(Set(otherSubscription, expectedSubscription))
  }

  test("unsubscribe before delay expires should should not invoke publisher") {
    val startTimestamp = Timestamp(1234)
    val cookie = "miam"

    currentCookie = cookie
    when(txLogProxy.getLastLoggedRecord).thenReturn(Some(Index(-1, Some(startTimestamp))))
    subscriber.subscribe(member, txLogProxy, delay = 500, subscribeAction, unsubscribeAction,
      ReplicationMode.Store, onSubscriptionEnd)

    // Ensure no interaction yet
    subscribeAction.verifyZeroInteractions(wait = 200)
    unsubscribeAction.verifyZeroInteractions()
    verifyOnSubscriptionEndNotCalled()
    subscriber.subscriptions should be(List(ReplicationSubscription(member, cookie, ReplicationMode.Store)))

    // Unsubscribe and wait end of the delay
    subscriber.unsubscribe(member)
    subscribeAction.verifyZeroInteractions(wait = 400)
    unsubscribeAction.verifyZeroInteractions()
    verifyOnSubscriptionEndNotCalled()
    subscriber.subscriptions should be(Nil)
  }

  test("unsubscribe pending publisher should ignore subscribe response") {
    val startTimestamp = Timestamp(1234)
    val cookie = "miam"
    val subId = "9876"
    val expectedSubscription = ReplicationSubscription(member, cookie, ReplicationMode.Store)

    currentCookie = cookie
    when(txLogProxy.getLastLoggedRecord).thenReturn(Some(Index(-1, Some(startTimestamp))))
    subscriber.subscribe(member, txLogProxy, delay = 0, subscribeAction, unsubscribeAction,
      ReplicationMode.Store, onSubscriptionEnd)

    val subscribeRequest: Map[String, MValue] = Map(
      ReplicationParam.Token -> token.toString,
      ReplicationParam.Cookie -> cookie,
      ReplicationParam.Start -> startTimestamp.toString,
      ReplicationParam.Mode -> ReplicationMode.Store.toString)

    val subscribeResponse: Map[String, MValue] = Map(
      ReplicationParam.SubscriptionId -> subId,
      ReplicationParam.Cookie -> cookie,
      ReplicationParam.Start -> startTimestamp.toString,
      ReplicationParam.End -> "5678")

    val unsubscribeRequest: Map[String, MValue] = Map(
      ReplicationParam.Token -> token.toString,
      ReplicationParam.SubscriptionId -> subId)

    // Verify subscription is pending (subscription reponse is delayed after unsubscribe)
    val matchCaptor = MessageMatcher(subscribeRequest)
    verify(subscribeAction.mockAction, timeout(100)).callOutgoingHandlers(argThat(matchCaptor))
    subscribeAction.verifyNoMoreInteractions(wait = 100)
    unsubscribeAction.verifyZeroInteractions()
    verifyOnSubscriptionEndNotCalled()
    subscriber.subscriptions should be(List(expectedSubscription))

    // Unsubscribe pending subscription before receiving response
    subscriber.unsubscribe(member)
    subscribeAction.verifyZeroInteractions(wait = 100)
    unsubscribeAction.verifyZeroInteractions()
    verifyOnSubscriptionEndNotCalled()
    subscriber.subscriptions should be(Nil)

    // Finally receive the subscribe response (after unsubscribing)
    matchCaptor.replyCapturedMessageWith(subscribeResponse)

    // Verify unsubscribe is sent after getting the late subscribe response
    verify(unsubscribeAction.mockAction, timeout(100)).callOutgoingHandlers(argThat(matchMessage(unsubscribeRequest)))
    subscribeAction.verifyZeroInteractions(wait = 100)
    unsubscribeAction.verifyNoMoreInteractions()
    verifyOnSubscriptionEndNotCalled()
    subscriber.subscriptions should be(Nil)
  }

  test("unsubscribe should unsubscribe proper subscription") {
    val startTimestamp = Timestamp(1234)
    val endTimestamp = Timestamp(5678)
    val cookie = "miam"
    val subId = "9876"
    val expectedSubscription = ReplicationSubscription(member, cookie, ReplicationMode.Store, id = Some(subId))

    currentCookie = cookie
    when(txLogProxy.getLastLoggedRecord).thenReturn(Some(Index(-1, Some(startTimestamp))))
    subscriber.subscribe(member, txLogProxy, delay = 0, subscribeAction, unsubscribeAction,
      ReplicationMode.Store, onSubscriptionEnd)
    val subscribeRequest: Map[String, MValue] = Map(
      ReplicationParam.Token -> token.toString,
      ReplicationParam.Cookie -> cookie,
      ReplicationParam.Start -> startTimestamp.toString,
      ReplicationParam.Mode -> ReplicationMode.Store.toString)

    val subscribeResponse: Map[String, MValue] = Map(
      ReplicationParam.SubscriptionId -> subId,
      ReplicationParam.Cookie -> cookie,
      ReplicationParam.Start -> startTimestamp.toString,
      ReplicationParam.End -> endTimestamp.toString)

    // Verify subscription is activated
    val matchCaptor = MessageMatcher(subscribeRequest)
    verify(subscribeAction.mockAction, timeout(100)).callOutgoingHandlers(argThat(matchCaptor))
    matchCaptor.replyCapturedMessageWith(subscribeResponse)
    subscribeAction.verifyNoMoreInteractions(wait = 100)
    unsubscribeAction.verifyZeroInteractions()
    verifyOnSubscriptionEndNotCalled()
    subscriber.subscriptions should be(List(expectedSubscription))

    // Unsubscribe member without subscription (should do nothing)
    val otherMember = ResolvedServiceMember(service.name, token + 1000, Seq(TokenRange.All))
    subscriber.unsubscribe(otherMember)
    unsubscribeAction.verifyZeroInteractions(wait = 100)
    subscribeAction.verifyZeroInteractions()
    subscriber.subscriptions should be(List(expectedSubscription))

    // Unsubscribe
    val unsubscribeRequest: Map[String, MValue] = Map(
      ReplicationParam.Token -> token.toString,
      ReplicationParam.SubscriptionId -> subId)

    subscriber.unsubscribe(member)
    verify(unsubscribeAction.mockAction, timeout(100)).callOutgoingHandlers(argThat(matchMessage(unsubscribeRequest)))
    unsubscribeAction.verifyNoMoreInteractions(wait = 100)
    subscribeAction.verifyZeroInteractions()
    subscriber.subscriptions should be(Nil)
  }

  test("publish should store in tx log and store in proper order") {
    val startTimestamp = 0L
    val subscription = subscribe(startTimestamp)

    val tx1 = createRequestMessage(timestamp = 100L)
    val tx2 = createRequestMessage(timestamp = 200L)
    val tx3 = createRequestMessage(timestamp = 300L)

    val ordered = inOrder(txLogProxy.mockAppender, mockStore)
    currentLogId = 1000

    // Publish transactions out of order
    subscriber.handlePublishMessage(createPublishMessage(sequence = 2, subscription.id.get, tx2))
    subscriber.handlePublishMessage(createPublishMessage(sequence = 3, subscription.id.get, tx3))
    subscriber.handlePublishMessage(createPublishMessage(sequence = 1, subscription.id.get, tx1))

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
    verifyNoMoreInteractions(txLogProxy.mockAppender)
    verifyNoMoreInteractions(mockStore)
    publishReplyCount should be (3)
    subscribeAction.verifyZeroInteractions()
    unsubscribeAction.verifyZeroInteractions()
  }

  test("publish tx log append error should result in consistency error") {
    val startTimestamp = 0L
    val subscription = subscribe(startTimestamp)

    currentLogId = 1000

    val tx1 = createRequestMessage(timestamp = 100L)
    val tx2 = createRequestMessage(timestamp = 200L)
    val tx3 = createRequestMessage(timestamp = 300L)

    val ordered = inOrder(txLogProxy.mockAppender, mockStore)
    when(txLogProxy.mockAppender.append(LogRecord(1002, tx1.timestamp, tx2))).thenThrow(new RuntimeException())

    // Publish transactions out of order
    subscriber.handlePublishMessage(createPublishMessage(sequence = 2, subscription.id.get, tx2))
    subscriber.handlePublishMessage(createPublishMessage(sequence = 1, subscription.id.get, tx1))
    subscriber.handlePublishMessage(createPublishMessage(sequence = 3, subscription.id.get, tx3))

    // Wait for transaction processing, unfortunatly timeout mode is not implemented with InOrder
    Thread.sleep(100)

    // Verify transactions applied in order up to the error
    ordered.verify(txLogProxy.mockAppender).append(LogRecord(1000, Some(startTimestamp), tx1))
    ordered.verify(mockStore).writeTransaction(argThat(matchMessage(tx1)))
    ordered.verify(txLogProxy.mockAppender).append(LogRecord(1001, Some(startTimestamp), createResponseMessage(tx1)))
    ordered.verify(txLogProxy.mockAppender).append(LogRecord(1002, tx1.timestamp, tx2))
    verifyNoMoreInteractions(txLogProxy.mockAppender)
    verifyNoMoreInteractions(mockStore)
    publishReplyCount should be (1)

    val unsubscribeRequest: Map[String, MValue] = Map(
      ReplicationParam.Token -> token.toString,
      ReplicationParam.SubscriptionId -> subscription.id.get)
    verify(unsubscribeAction.mockAction, timeout(100)).callOutgoingHandlers(argThat(matchMessage(unsubscribeRequest)))
    subscribeAction.verifyZeroInteractions()
  }

  test("publish consistent store write error should result in consistency error") {
    val startTimestamp = 0L
    val subscription = subscribe(startTimestamp)

    currentLogId = 1000

    val tx1 = createRequestMessage(timestamp = 100L)
    val tx2 = createRequestMessage(timestamp = 200L)

    val ordered = inOrder(txLogProxy.mockAppender, mockStore)
    when(mockStore.writeTransaction(argThat(matchMessage(tx1)))).thenThrow(new RuntimeException())

    // Publish transactions out of order
    subscriber.handlePublishMessage(createPublishMessage(sequence = 2, subscription.id.get, tx2))
    subscriber.handlePublishMessage(createPublishMessage(sequence = 1, subscription.id.get, tx1))

    // Wait for transaction processing, unfortunatly timeout mode is not implemented with InOrder
    Thread.sleep(100)

    // Verify transactions applied in order up to the error
    ordered.verify(txLogProxy.mockAppender).append(LogRecord(1000, Some(startTimestamp), tx1))
    ordered.verify(mockStore).writeTransaction(argThat(matchMessage(tx1)))
    verifyNoMoreInteractions(txLogProxy.mockAppender)
    verifyNoMoreInteractions(mockStore)
    publishReplyCount should be (0)

    val unsubscribeRequest: Map[String, MValue] = Map(
      ReplicationParam.Token -> token.toString,
      ReplicationParam.SubscriptionId -> subscription.id.get)
    verify(unsubscribeAction.mockAction, timeout(100)).callOutgoingHandlers(argThat(matchMessage(unsubscribeRequest)))
    subscribeAction.verifyZeroInteractions()
  }

  test("skip publish sequence # should result in consistency error") {
    val startTimestamp = 0L
    val subscription = subscribe(startTimestamp)

    currentLogId = 1000

    val tx1 = createRequestMessage(timestamp = 100L)
    val tx2 = createRequestMessage(timestamp = 200L)
    val tx3 = createRequestMessage(timestamp = 300L)

    val ordered = inOrder(txLogProxy.mockAppender, mockStore)

    // Publish transactions with a sequence # gap, untill timeout and resume
    subscriber.handlePublishMessage(createPublishMessage(sequence = 3, subscription.id.get, tx3))
    subscriber.handlePublishMessage(createPublishMessage(sequence = 1, subscription.id.get, tx1))
    Thread.sleep((subscriptionTimeout * 1.5).toLong)
    subscriber.handlePublishMessage(createPublishMessage(sequence = 2, subscription.id.get, tx2))

    // Verify transactions applied in order up to the timeout
    ordered.verify(txLogProxy.mockAppender).append(LogRecord(1000, Some(startTimestamp), tx1))
    ordered.verify(mockStore).writeTransaction(argThat(matchMessage(tx1)))
    ordered.verify(txLogProxy.mockAppender).append(LogRecord(1001, Some(startTimestamp), createResponseMessage(tx1)))
    verifyNoMoreInteractions(txLogProxy.mockAppender)
    verifyNoMoreInteractions(mockStore)
    publishReplyCount should be (1)

    val unsubscribeRequest: Map[String, MValue] = Map(
      ReplicationParam.Token -> token.toString,
      ReplicationParam.SubscriptionId -> subscription.id.get)
    verify(unsubscribeAction.mockAction, timeout(100)).callOutgoingHandlers(argThat(matchMessage(unsubscribeRequest)))
    subscribeAction.verifyZeroInteractions()
  }

  test("stop publish should result in timeout error") {
    val startTimestamp = 0L
    val subscription = subscribe(startTimestamp)

    currentLogId = 1000

    val tx1 = createRequestMessage(timestamp = 100L)

    val ordered = inOrder(txLogProxy.mockAppender, mockStore)

    // Publish transactions with a sequence # gap, untill timeout and resume
    subscriber.handlePublishMessage(createPublishMessage(sequence = 1, subscription.id.get, tx1))
    Thread.sleep((subscriptionTimeout * 1.5).toLong)

    // Verify transactions applied in order up to the timeout
    ordered.verify(txLogProxy.mockAppender).append(LogRecord(1000, Some(startTimestamp), tx1))
    ordered.verify(mockStore).writeTransaction(argThat(matchMessage(tx1)))
    ordered.verify(txLogProxy.mockAppender).append(LogRecord(1001, Some(startTimestamp), createResponseMessage(tx1)))
    verifyNoMoreInteractions(txLogProxy.mockAppender)
    verifyNoMoreInteractions(mockStore)
    publishReplyCount should be (1)

    val unsubscribeRequest: Map[String, MValue] = Map(
      ReplicationParam.Token -> token.toString,
      ReplicationParam.SubscriptionId -> subscription.id.get)
    verify(unsubscribeAction.mockAction, timeout(100)).callOutgoingHandlers(argThat(matchMessage(unsubscribeRequest)))
    subscribeAction.verifyZeroInteractions()
  }

  test("publish idle message should not result in timeout error") {
    val startTimestamp = 0L
    val subscription = subscribe(startTimestamp)
    currentLogId = 1000

    // Publish keep alive messages
    subscriber.handlePublishMessage(createKeepAliveMessage(sequence = 1, subscription.id.get))
    Thread.sleep(subscriptionTimeout / 2)
    subscriber.handlePublishMessage(createKeepAliveMessage(sequence = 2, subscription.id.get))
    Thread.sleep(subscriptionTimeout / 2)
    subscriber.handlePublishMessage(createKeepAliveMessage(sequence = 3, subscription.id.get))
    Thread.sleep(subscriptionTimeout / 2)

    // Wait for transaction processing
    Thread.sleep(100)

    verifyZeroInteractions(txLogProxy.mockAppender)
    verifyZeroInteractions(mockStore)
    publishReplyCount should be (3)
    subscribeAction.verifyZeroInteractions()
    unsubscribeAction.verifyZeroInteractions()
  }
}