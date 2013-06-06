package com.wajam.nrv.consistency.replication

import org.scalatest.{BeforeAndAfter, FunSuite}
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._
import org.scalatest.matchers.ShouldMatchers._
import org.mockito.Matchers._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import com.wajam.nrv.service._
import com.wajam.nrv.consistency.{TransactionLogProxy, ResolvedServiceMember, ConsistentStore}
import com.wajam.nrv.data._
import com.wajam.nrv.utils.timestamp.Timestamp
import MessageMatcher._
import com.wajam.nrv.consistency.persistence.LogRecord.Index
import scala.Some

@RunWith(classOf[JUnitRunner])
class TestReplicationSubscriber extends FunSuite with BeforeAndAfter with MockitoSugar {

  var service: Service = null
  var member: ResolvedServiceMember = null
  var txLogProxy: TransactionLogProxy = null
  var mockStore: ConsistentStore = null
  var subscribeAction: ActionProxy = null
  var unsubscribeAction: ActionProxy = null

  var currentCookie: String = null
  var subscriber: ReplicationSubscriber = null

  val subscriptionTimeout = 30000L
  val token = 0
  var subscriptionEndErrors: List[Exception] = Nil
  var subscriptionEndCalls = 0

  before {
    service = new Service("service")
    member = ResolvedServiceMember(service.name, token, Seq(TokenRange.All))
    txLogProxy = new TransactionLogProxy

    mockStore = mock[ConsistentStore]
    subscribeAction = ActionProxy()
    unsubscribeAction = ActionProxy()

    subscriber = new ReplicationSubscriber(service, mockStore, subscriptionTimeout, commitFrequency = 0) {
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

  test("subscribe should invoke master publisher") {
    val startTimestamp = Timestamp(1234)
    val cookie = "miam"

    currentCookie = cookie
    when(txLogProxy.getLastLoggedRecord).thenReturn(Some(Index(-1, Some(startTimestamp))))
    subscriber.subscribe(member, txLogProxy, delay = 0, subscribeAction, unsubscribeAction,
      ReplicationMode.Store, onSubscriptionEnd)
    val subscribeRequest: Map[String, MValue] = Map(
      (ReplicationParam.Token -> token.toString),
      (ReplicationParam.Cookie -> cookie),
      (ReplicationParam.Start -> startTimestamp.toString),
      (ReplicationParam.Mode -> ReplicationMode.Store.toString))

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
      (ReplicationParam.Token -> token.toString),
      (ReplicationParam.Cookie -> cookie),
      (ReplicationParam.Start -> startTimestamp.toString),
      (ReplicationParam.Mode -> ReplicationMode.Store.toString))

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
      (ReplicationParam.Token -> token.toString),
      (ReplicationParam.Cookie -> cookie),
      (ReplicationParam.Start -> startTimestamp.toString),
      (ReplicationParam.Mode -> ReplicationMode.Store.toString))

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
      (ReplicationParam.Token -> token.toString),
      (ReplicationParam.Cookie -> cookie),
      (ReplicationParam.Start -> startTimestamp.toString),
      (ReplicationParam.Mode -> ReplicationMode.Store.toString))

    val subscribeResponse: Map[String, MValue] = Map(
      (ReplicationParam.SubscriptionId -> subId),
      (ReplicationParam.Cookie -> cookie),
      (ReplicationParam.Start -> startTimestamp.toString),
      (ReplicationParam.End -> endTimestamp.toString))


    val messageCaptor = MessageMatcher(subscribeRequest)
    verify(subscribeAction.mockAction, timeout(100)).callOutgoingHandlers(argThat(messageCaptor))
    messageCaptor.replyWith(subscribeResponse)
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
      (ReplicationParam.Token -> token.toString),
      (ReplicationParam.Cookie -> cookie),
      (ReplicationParam.Start -> startTimestamp.toString),
      (ReplicationParam.Mode -> ReplicationMode.Store.toString))

    val subscribeResponse: Map[String, MValue] = Map(
      (ReplicationParam.SubscriptionId -> subId),
      (ReplicationParam.Cookie -> "bad cookie"),
      (ReplicationParam.Start -> startTimestamp.toString),
      (ReplicationParam.End -> endTimestamp.toString))

    val unsubscribeRequest: Map[String, MValue] = Map(
      (ReplicationParam.Token -> token.toString),
      (ReplicationParam.SubscriptionId -> subId))

    val messageCaptor = MessageMatcher(subscribeRequest)
    verify(subscribeAction.mockAction, timeout(100)).callOutgoingHandlers(argThat(messageCaptor))
    messageCaptor.replyWith(subscribeResponse)
    subscribeAction.verifyNoMoreInteractions(wait = 100)
    verify(unsubscribeAction.mockAction, timeout(100)).callOutgoingHandlers(argThat(matchMessage(unsubscribeRequest)))
    unsubscribeAction.verifyNoMoreInteractions()
    verifyOnSubscriptionEndNotCalled()
    subscriber.subscriptions should be(List(expectedSubscription))
  }

  test("subscribe response error should remove pending subscription") {
    val startTimestamp = Timestamp(1234)
    val cookie = "miam"
    val subId = "9876"

    currentCookie = cookie
    when(txLogProxy.getLastLoggedRecord).thenReturn(Some(Index(-1, Some(startTimestamp))))
    subscriber.subscribe(member, txLogProxy, delay = 0, subscribeAction, unsubscribeAction,
      ReplicationMode.Store, onSubscriptionEnd)
    val subscribeRequest: Map[String, MValue] = Map(
      (ReplicationParam.Token -> token.toString),
      (ReplicationParam.Cookie -> cookie),
      (ReplicationParam.Start -> startTimestamp.toString),
      (ReplicationParam.Mode -> ReplicationMode.Store.toString))

    val subscribeResponse = new Exception("reponse error")

    // Verify onSubscriptionEnd is invoked with the error
    val messageCaptor = MessageMatcher(subscribeRequest)
    verify(subscribeAction.mockAction, timeout(100)).callOutgoingHandlers(argThat(messageCaptor))
    messageCaptor.replyWith(subscribeResponse)
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
      (ReplicationParam.Token -> token.toString),
      (ReplicationParam.Cookie -> cookie),
      (ReplicationParam.Start -> startTimestamp.toString),
      (ReplicationParam.Mode -> ReplicationMode.Store.toString))

    val subscribeResponse: Map[String, MValue] = Map(
      (ReplicationParam.SubscriptionId -> subId),
      (ReplicationParam.Cookie -> cookie),
      (ReplicationParam.Start -> startTimestamp.toString),
      (ReplicationParam.End -> startTimestamp.toString))

    val unsubscribeRequest: Map[String, MValue] = Map(
      (ReplicationParam.Token -> token.toString),
      (ReplicationParam.SubscriptionId -> subId))

    // Verify unsubscribe is sent and onSubscriptionEnd is invoked without an error
    val messageCaptor = MessageMatcher(subscribeRequest)
    verify(subscribeAction.mockAction, timeout(100)).callOutgoingHandlers(argThat(messageCaptor))
    messageCaptor.replyWith(subscribeResponse)
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
      (ReplicationParam.Token -> token.toString),
      (ReplicationParam.Cookie -> cookie),
      (ReplicationParam.Start -> startTimestamp.toString),
      (ReplicationParam.Mode -> ReplicationMode.Store.toString))

    val subscribeResponse: Map[String, MValue] = Map(
      (ReplicationParam.SubscriptionId -> subId),
      (ReplicationParam.Cookie -> cookie),
      (ReplicationParam.Start -> startTimestamp.toString),
      (ReplicationParam.End -> endTimestamp.toString))

    val messageCaptor = MessageMatcher(subscribeRequest)
    verify(subscribeAction.mockAction, timeout(100)).callOutgoingHandlers(argThat(messageCaptor))
    messageCaptor.replyWith(subscribeResponse)
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
      (ReplicationParam.Token -> token.toString),
      (ReplicationParam.Cookie -> cookie),
      (ReplicationParam.Start -> startTimestamp.toString),
      (ReplicationParam.Mode -> ReplicationMode.Store.toString))

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
      (ReplicationParam.Token -> token.toString),
      (ReplicationParam.Cookie -> cookie),
      (ReplicationParam.Start -> startTimestamp.toString),
      (ReplicationParam.Mode -> ReplicationMode.Store.toString))

    val subscribeResponse: Map[String, MValue] = Map(
      (ReplicationParam.SubscriptionId -> subId),
      (ReplicationParam.Cookie -> cookie),
      (ReplicationParam.Start -> startTimestamp.toString),
      (ReplicationParam.End -> "5678"))

    val unsubscribeRequest: Map[String, MValue] = Map(
      (ReplicationParam.Token -> token.toString),
      (ReplicationParam.SubscriptionId -> subId))

    // Verify subscription is pending (subscription reponse is delayed in the future)
    val messageCaptor = MessageMatcher(subscribeRequest)
    verify(subscribeAction.mockAction, timeout(100)).callOutgoingHandlers(argThat(messageCaptor))
    messageCaptor.replyWith(subscribeResponse, delay = 500)
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

    // Ensure unsubscribe is sent when getting the subscribe response
    subscribeAction.verifyZeroInteractions(wait = 500)
    verify(unsubscribeAction.mockAction, timeout(100)).callOutgoingHandlers(argThat(matchMessage(unsubscribeRequest)))
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
      (ReplicationParam.Token -> token.toString),
      (ReplicationParam.Cookie -> cookie),
      (ReplicationParam.Start -> startTimestamp.toString),
      (ReplicationParam.Mode -> ReplicationMode.Store.toString))

    val subscribeResponse: Map[String, MValue] = Map(
      (ReplicationParam.SubscriptionId -> subId),
      (ReplicationParam.Cookie -> cookie),
      (ReplicationParam.Start -> startTimestamp.toString),
      (ReplicationParam.End -> endTimestamp.toString))

    // Verify subscription is activated
    val messageCaptor = MessageMatcher(subscribeRequest)
    verify(subscribeAction.mockAction, timeout(100)).callOutgoingHandlers(argThat(messageCaptor))
    messageCaptor.replyWith(subscribeResponse)
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
      (ReplicationParam.Token -> token.toString),
      (ReplicationParam.SubscriptionId -> subId))

    subscriber.unsubscribe(member)
    verify(unsubscribeAction.mockAction, timeout(100)).callOutgoingHandlers(argThat(matchMessage(unsubscribeRequest)))
    unsubscribeAction.verifyNoMoreInteractions(wait = 100)
    subscribeAction.verifyZeroInteractions()
    subscriber.subscriptions should be(Nil)
  }

  ignore("publish should store in tx log and store in proper order") {

  }

  ignore("publish tx log append error should result in consistency error") {
    // TODO: must not write in store
  }

  ignore("publish consistent store write error should result in consistency error") {
    // TODO: must not response in tx log
  }

  ignore("skip publish sequence # should result in consistency error") {

  }

  ignore("stop publish should result in timeout error") {

  }

  ignore("publish idle message should not result in timeout error") {

  }
}