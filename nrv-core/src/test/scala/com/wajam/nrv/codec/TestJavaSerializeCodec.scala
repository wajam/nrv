package com.wajam.nrv.codec

import org.scalatest.FunSuite
import com.wajam.nrv.cluster.Node
import com.wajam.nrv.service.{ServiceMember, Endpoints}
import com.wajam.nrv.data.{Message, OutRequest}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TestJavaSerializeCodec extends FunSuite {
  test("serialize, unserialize") {
    val codec = new JavaSerializeCodec()

    val req = new OutRequest(Map("test" -> "someval"))
    req.destination = Endpoints.list(new ServiceMember(0, new Node("127.0.0.1", Map("nrv" -> 12345))))
    val bytes = codec.encode(req)

    assert(bytes.length > 0)

    val decoded = codec.decode(bytes)

    assert(decoded != null)

    decoded match {
      case m: Message =>
        assert(m.getOrElse("test", "") == "someval")
      case _ => fail("Wasn't decoded to message")
    }

  }

}
