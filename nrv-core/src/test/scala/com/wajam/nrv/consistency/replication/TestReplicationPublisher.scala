package com.wajam.nrv.consistency.replication

import org.scalatest.BeforeAndAfter
import org.scalatest.mock.MockitoSugar
import org.scalatest.matchers.ShouldMatchers._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import com.wajam.nrv.service._
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

@RunWith(classOf[JUnitRunner])
class TestReplicationPublisher extends TestTransactionBase with BeforeAndAfter with MockitoSugar {

  var service: Service = null
  var member: ResolvedServiceMember = null
  var mockStore: ConsistentStore = null
  var publishAction: ActionProxy = null

  var subscriptionId: String = "id"
  var currentConsistentTimestamp: Option[Timestamp] = None
  var publisher: ReplicationPublisher = null

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
    publishAction = ActionProxy()

    publisher = new ReplicationPublisher(service, mockStore, (_) => txLog, (_) => currentConsistentTimestamp,
      publishAction, publishTps = 20, publishWindowSize = 2, subscriptionTimeout) {
      override def nextId = subscriptionId
    }
    publisher.start()
  }

  after {
    publisher.stop()
    publisher = null
    publishAction = null
    mockStore = null

    txLog.close()
    txLog = null

    logDir.listFiles().foreach(_.delete())
    logDir.delete()
    logDir = null

    currentConsistentTimestamp = None
  }

  def appendTransactions(count: Int, initialTimestamp: Long = 0, timestampIncrement: Int = 1): List[LogRecord] = {

    def append(nextId: Long, timestamp: Long, consistentTimestamp: Option[Timestamp], remaining: Int): List[LogRecord] = {
      if (remaining > 0) {
        val message = createRequestMessage(timestamp)
        val request = txLog.append(LogRecord(nextId, consistentTimestamp, message))
        val response = txLog.append(LogRecord(request.id + 1, consistentTimestamp, createResponseMessage(message)))
        println(request)
        println(response)
        request :: response :: append(response.id + 1, timestamp + timestampIncrement, Some(timestamp), remaining - 1)
      } else {
        val index = txLog.append(Index(nextId, consistentTimestamp))
        println(index)
        index :: Nil
      }
    }

    append(0, initialTimestamp, None, count)
  }

  test("subscribe should fail if not local service member") {
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
    publishAction.verifyZeroInteractions()
  }

  test("subscribe should fail if no service member") {
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
    publishAction.verifyZeroInteractions()
  }

  test("subscribe with live replication mode with start timestamp") {
    val cookie = "" // TODO: proper cookie
    val expectedSubscription = ReplicationSubscription(member, cookie, ReplicationMode.Live,
      id = Some(subscriptionId), startTimestamp = Some(0))

    val allRecords = appendTransactions(count = 5, initialTimestamp = 0)
    currentConsistentTimestamp = allRecords.last.consistentTimestamp
    val publishRequests = allRecords.collect{case r: Request => r}.slice(1, Int.MaxValue).map(r => {
      val publishParams: Map[String, MValue] = Map(
        ReplicationParam.Timestamp -> r.timestamp.toString,
        ReplicationParam.SubscriptionId -> subscriptionId,
        ReplicationParam.Sequence -> 1)
      new OutMessage(publishParams, data = r.message)
    })

    val subscribeRequest: Map[String, MValue] = Map(
      ReplicationParam.Token -> token.toString,
      ReplicationParam.Cookie -> cookie,
      ReplicationParam.Start -> "0",
      ReplicationParam.Mode -> ReplicationMode.Live.toString)

    val subscribeRequestMessage = new InMessage(subscribeRequest)
    var subscribeResponseMessage: Option[OutMessage] = None
    subscribeRequestMessage.replyCallback = (reply) => subscribeResponseMessage = Some(reply)

    publisher.handleSubscribeMessage(subscribeRequestMessage)
    publisher.subscriptions should be(List(expectedSubscription)) // synchronized actor message
    subscribeResponseMessage.get.error should be(None)
    verify(publishAction.mockAction, timeout(5000)).callOutgoingHandlers(argThat(matchMessage(publishRequests.head)))
    publishAction.verifyNoMoreInteractions()
  }

  ignore("subscribe with live replication mode without start timestamp") {

  }

  test("subscribe with live replication mode but no log") {
    val subscribeRequest: Map[String, MValue] = Map(
      ReplicationParam.Token -> token.toString,
      ReplicationParam.Cookie -> "cookie",
      ReplicationParam.Start -> "12345",
      ReplicationParam.Mode -> ReplicationMode.Live.toString)

    val subscribeRequestMessage = new InMessage(subscribeRequest)
    var subscribeResponseMessage: Option[OutMessage] = None
    subscribeRequestMessage.replyCallback = (reply) => subscribeResponseMessage = Some(reply)

    publisher.handleSubscribeMessage(subscribeRequestMessage)
    publisher.subscriptions should be(Nil) // synchronized actor message
    subscribeResponseMessage.get.error should not be None
    publishAction.verifyZeroInteractions()
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
