package com.wajam.nrv.protocol.codec

import org.scalatest.FunSuite
import com.wajam.nrv.cluster.Node
import com.wajam.nrv.data._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import com.wajam.nrv.service.{Replica, Endpoints, Shard}

@RunWith(classOf[JUnitRunner])
class TestJavaSerializeCodec extends FunSuite {
  test("serialize, unserialize") {
    val codec = new JavaSerializeCodec()

    val req = new OutMessage(Map("test" -> "someval"))
    req.destination = new Endpoints(Seq(new Shard(0, Seq(new Replica(0, new Node("127.0.0.1", Map("nrv" -> 12345)))))))
    val bytes = codec.encode(req)

    assert(bytes.length > 0)

    val decoded = codec.decode(bytes)

    assert(decoded != null)

    decoded match {
      case m: Message =>
        assert(m.parameters.getOrElse("test", "") == "someval")
      case _ => fail("Wasn't decoded to message")
    }

  }

}
