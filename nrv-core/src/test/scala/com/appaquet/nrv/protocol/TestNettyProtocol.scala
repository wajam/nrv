package com.appaquet.nrv.protocol

import org.scalatest.FunSuite
import com.appaquet.nrv.cluster.{Node, Cluster}
import com.appaquet.nrv.codec.JavaSerializeCodec
import com.appaquet.nrv.service.Action
import com.appaquet.nrv.data.{Message, OutRequest}

class TestNettyProtocol extends FunSuite {

  test("out-in") {
    val notifier = new Object()
    var received: Message = null

    val cluster = new Cluster(new Node("127.0.0.1", Map("nrv" -> 12345, "test" -> 12346)))
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

    protocol.handleOutgoing(null, new OutRequest(Map("test" -> "someval")))

    cluster.stop()

    notifier.synchronized {
      notifier.wait(10)
    }

    assert(received != null)
    assert(received.getOrElse("test", "") == "someval")
  }
}
