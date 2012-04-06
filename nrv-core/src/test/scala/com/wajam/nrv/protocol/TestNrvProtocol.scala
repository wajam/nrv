package com.wajam.nrv.protocol

import com.wajam.nrv.service.{ServiceMember, Endpoints, Action}
import com.wajam.nrv.data.{Message, OutRequest}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import com.wajam.nrv.cluster.{StaticClusterManager, Node, Cluster}
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSuite}

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
      override def handleIncoming(action: Action, message: Message) {
        received = message

        notifier.synchronized {
          notifier.notify()
        }
      }
    }
    cluster.registerProtocol(protocol)

    cluster.start()

    val req = new OutRequest(Map("test" -> "someval"))
    req.destination = new Endpoints(Seq(new ServiceMember(0, cluster.localNode)))
    protocol.handleOutgoing(null, req)

    notifier.synchronized {
      notifier.wait(100)
    }

    assert(received != null)
    assert(received.getOrElse("test", "") == "someval")
  }

  after {
    cluster.stop()
  }
}
