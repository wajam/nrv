package com.wajam.nrv.protocol

import org.scalatest.FunSuite
import com.wajam.nrv.codec.JavaSerializeCodec
import com.wajam.nrv.service.{ServiceMember, Endpoints, Action}
import com.wajam.nrv.data.{Message, OutRequest}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import com.wajam.nrv.cluster.{StaticClusterManager, Node, Cluster}

@RunWith(classOf[JUnitRunner])
class TestNettyProtocol extends FunSuite {
  test("out-in") {
    val notifier = new Object()
    var received: Message = null

    val cluster = new Cluster(new Node("127.0.0.1", Map("nrv" -> 12345, "test" -> 12346)), new StaticClusterManager)
    val protocol = new NettyProtocol("test", cluster, new JavaSerializeCodec()) {
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
    req.destination = Endpoints.list(new ServiceMember(0, cluster.localNode))
    protocol.handleOutgoing(null, req)

    cluster.stop()

    notifier.synchronized {
      notifier.wait(10)
    }

    assert(received != null)
    assert(received.getOrElse("test", "") == "someval")
  }
}
