package com.wajam.nrv.protocol

import com.wajam.nrv.service.{ServiceMember, Endpoints, Action}
import com.wajam.nrv.data.{Message, OutMessage}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import com.wajam.nrv.cluster.{StaticClusterManager, Node, Cluster}
import org.scalatest.{BeforeAndAfter, FunSuite}

@RunWith(classOf[JUnitRunner])
class TestNrvProtocol extends FunSuite with BeforeAndAfter {

  var cluster: Cluster = null

  before {
    cluster = new Cluster(new Node("127.0.0.1", Map("nrv" -> 12345, "test" -> 12346)), new StaticClusterManager)
  }

  test("out-in") {
    val notifier = new Object()
    var received: Message = null

    val protocol = new NrvProtocol(cluster) {

      override def parse(message: AnyRef): Message = {
        received = message.asInstanceOf[Message]

        notifier.synchronized {
          notifier.notify()
        }
        null
      }
    }
    cluster.registerProtocol(protocol)

    cluster.start()

    val req = new OutMessage(Map("test" -> "someval"))
    req.destination = new Endpoints(Seq(new ServiceMember(0, cluster.localNode)))
    protocol.handleOutgoing(null, req)

    notifier.synchronized {
      notifier.wait(100)
    }

    assert(received != null)
    assert(received.getOrElse("test", "") == "someval")
  }

  test("test connection failure") {

    val notifier = new Object()
    var received: Message = null
    val protocol = new NrvProtocol(cluster) {
      override def handleIncoming(action: Action, message: Message) {
        received = message

        notifier.synchronized {
          notifier.notify()
        }
      }
    }
    val req = new OutMessage(Map("test" -> "someval"))
    req.destination = new Endpoints(Seq(new ServiceMember(0, cluster.localNode)))
    protocol.handleOutgoing(null, req)

    notifier.synchronized {
      notifier.wait(100)
    }

    assert(received.error != None)
  }

  after {
    cluster.stop()
  }
}
