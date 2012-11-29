package com.wajam.nrv.protocol

import com.wajam.nrv.data._
import com.wajam.nrv.service._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import com.wajam.nrv.cluster.{LocalNode, StaticClusterManager, Cluster}
import org.scalatest.{BeforeAndAfter, FunSuite}

@RunWith(classOf[JUnitRunner])
class TestNrvProtocol extends FunSuite with BeforeAndAfter {

  var cluster: Cluster = null

  before {
    cluster = new Cluster(new LocalNode("127.0.0.1", Map("nrv" -> 12345, "test" -> 12346)), new StaticClusterManager)
  }

  test("out-in") {
    val notifier = new Object()
    var received: Message = null

    val protocol = new NrvProtocol(cluster.localNode) {

      override def parse(message: AnyRef): Message = {
        val parsedMsg = super.parse(message)
        received = parsedMsg

        notifier.synchronized {
          notifier.notify()
        }
        null
      }
    }
    cluster.registerProtocol(protocol)

    cluster.start()

    val req = new OutMessage(Map("test" -> "someval"))
    req.destination = new Endpoints(Seq(new Shard(0, Seq(new Replica(0, cluster.localNode)))))
    protocol.handleOutgoing(null, req)

    notifier.synchronized {
      notifier.wait(100)
    }

    assert(received != null)
    assert(received.parameters.getOrElse("test", "") == "someval")

    cluster.stop()
  }

  test("test connection failure") {

    val notifier = new Object()
    var received: Message = null
    val protocol = new NrvProtocol(cluster.localNode) {
      override def handleIncoming(action: Action, message: InMessage) {
        received = message

        notifier.synchronized {
          notifier.notify()
        }
      }
    }
    val req = new OutMessage(Map("test" -> "someval"))
    req.destination = new Endpoints(Seq(new Shard(0, Seq(new Replica(0, cluster.localNode)))))
    protocol.handleOutgoing(null, req)

    notifier.synchronized {
      notifier.wait(100)
    }

    assert(received.error != None)
  }

  test("test message reception failure") {

    val protocol = new NrvProtocol(cluster.localNode) {
      override def parse(message: AnyRef): Message = {
        throw new RuntimeException
      }

      override def handleIncoming(action: Action, message: InMessage) {
        fail("should not call handle incoming")
      }

      override def handleOutgoing(action: Action, message: OutMessage) {
        fail("should not call handle outgoing")
      }
    }

    protocol.transportMessageReceived("invalidmessage".getBytes, None)
  }

  test("test message parsing failure") {

    val protocol = new NrvProtocol(cluster.localNode) {
      override def parse(message: AnyRef): Message = {
        throw new ParsingException("400")
      }

      override def handleIncoming(action: Action, message: InMessage) {
        fail("should not call handle incoming")
      }

      override def handleOutgoing(action: Action, message: OutMessage) {
        fail("should not call handle outgoing")
      }
    }

    protocol.transportMessageReceived("invalidmessage".getBytes, None)
  }

  after {
    cluster.stop()
  }
}
