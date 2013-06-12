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
import java.util.UUID
import com.wajam.nrv.utils.IdGenerator

@RunWith(classOf[JUnitRunner])
class TestReplicationPublisher extends TestTransactionBase with BeforeAndAfter with MockitoSugar {

  var service: Service = null
  var member: ResolvedServiceMember = null
  var txLogProxy: TransactionLogProxy = null
  var mockStore: ConsistentStore = null
  var publishAction: ActionProxy = null

  var subscriptionId: String = null
  var currentConsistentTimestamp: Long = 0
  var publisher: ReplicationPublisher = null

  val subscriptionTimeout = 1500L
  val token = 0

  before {
    service = new Service("service") // TODO: initialize cluster with local and remove service member nodes
    member = ResolvedServiceMember(service.name, token, Seq(TokenRange.All))
    txLogProxy = new TransactionLogProxy

    mockStore = mock[ConsistentStore]
    publishAction = ActionProxy()

    publisher = new ReplicationPublisher(service, mockStore, (_) => txLogProxy, (_) => Some(currentConsistentTimestamp),
      publishAction, publishTps = 1, publishWindowSize = 1, subscriptionTimeout) {
      override def nextId = subscriptionId
    }
    publisher.start()
  }

  after {
    publisher.stop()
    publisher = null
    publishAction = null
    mockStore = null
    txLogProxy = null

    currentConsistentTimestamp = 0L
  }

  ignore("subscribe should fail if not local service member") {

  }

  ignore("subscribe should fail if no service member") {

  }

  ignore("subscribe with live replication mode with start timestamp") {

  }

  ignore("subscribe with live replication mode without start timestamp") {

  }

  ignore("subscribe with live replication mode but no log") {

  }

  ignore("subscribe with live replication mode but log after start timestamp") {

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
