package com.appaquet.nrv.codec

import org.scalatest.FunSuite
import com.appaquet.nrv.data.{Message, OutRequest}

class TestJavaSerializeCodec extends FunSuite {
  test("serialize, unserialize") {
    val codec = new JavaSerializeCodec()

    val bytes = codec.encode(new OutRequest(Map("test" -> "someval")))

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
