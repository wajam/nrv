package com.wajam.nrv.consistency

import org.scalatest.FunSuite
import com.wajam.nrv.service.{Replica, Shard, Endpoints, Action}
import org.scalatest.mock.MockitoSugar
import org.scalatest.Matchers._
import org.mockito.Mockito._
import com.wajam.nrv.data.OutMessage
import com.wajam.nrv.cluster.Node
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TestConsistencyOne extends FunSuite with MockitoSugar {

  def replica(token: Int = 0, host: String = "localhost", port: Int = 1234, selected: Boolean = true) = {
    new Replica(token, new Node("localhost", Map("nrv" -> port)), selected)
  }

  test("one selected replica should stay selected") {
    var mockAction = mock[Action]
    val message = new OutMessage()

    val replicas = Seq(replica())
    replicas.map(_.selected) should be(Seq(true))
    val shard = new Shard(0, replicas)
    message.destination = new Endpoints(Seq(shard))

    val consistency = new ConsistencyOne
    consistency.handleOutgoing(mockAction, message)
    verifyZeroInteractions(mockAction)
    replicas.map(_.selected) should be(Seq(true))
  }

  test("first selected replica should stay selected but not the others") {
    var mockAction = mock[Action]
    val message = new OutMessage()

    val replicas = Seq(replica(port = 1), replica(port = 2))
    replicas.map(_.selected) should be(Seq(true, true))
    val shard = new Shard(0, replicas)
    message.destination = new Endpoints(Seq(shard))

    val consistency = new ConsistencyOne
    consistency.handleOutgoing(mockAction, message)
    verifyZeroInteractions(mockAction)
    replicas.map(_.selected) should be(Seq(true, false))
  }

  test("first selected replica should stay selected but not the others - first initially non-selected") {
    var mockAction = mock[Action]
    val message = new OutMessage()

    val replicas = Seq(replica(port = 1, selected = false), replica(port = 2), replica(port = 3))
    replicas.map(_.selected) should be(Seq(false, true, true))
    val shard = new Shard(0, replicas)
    message.destination = new Endpoints(Seq(shard))

    val consistency = new ConsistencyOne
    consistency.handleOutgoing(mockAction, message)
    verifyZeroInteractions(mockAction)
    replicas.map(_.selected) should be(Seq(false, true, false))
  }

  test("stay non-selected if none selected") {
    var mockAction = mock[Action]
    val message = new OutMessage()

    val replicas = Seq(replica(selected = false))
    replicas.map(_.selected) should be(Seq(false))
    val shard = new Shard(0, replicas)
    message.destination = new Endpoints(Seq(shard))

    val consistency = new ConsistencyOne
    consistency.handleOutgoing(mockAction, message)
    verifyZeroInteractions(mockAction)
    replicas.map(_.selected) should be(Seq(false))
  }
}
