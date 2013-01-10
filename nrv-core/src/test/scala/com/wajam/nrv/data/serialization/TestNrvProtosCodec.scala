package com.wajam.nrv.data.serialization

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FunSuite
import org.scalatest.matchers.ShouldMatchers._

@RunWith(classOf[JUnitRunner])
class TestNrvProtosCodec extends FunSuite {

  test("can encode message") {
    sys.error("unimplemented")
  }

  test("can decode message") {
    sys.error("unimplemented")
  }

  test("can encode node") {
    sys.error("unimplemented")
  }

  test("can decode node") {
    sys.error("unimplemented")
  }

  test("can encode endpoints") {
    sys.error("unimplemented")
  }

  test("can decode endpoints") {
    sys.error("unimplemented")
  }

  test("can encode shards") {
    sys.error("unimplemented")
  }

  test("can decode shards") {
    sys.error("unimplemented")
  }

  test("can encode using codec") {
    sys.error("unimplemented")
  }

  test("can decode using codec") {
    sys.error("unimplemented")
  }

  def generateException() = {
    var exception: Exception = null

    try {
      val x, y = 0
      val z = x / y
    }
    catch {
      case ex:Exception => exception = ex
    }

    exception
  }

  test("can serialize exception") {

    val codec = new NrvProtosCodec()

    val bytes = codec.serializeToBytes(generateException())

    assert(bytes != Array(Byte), "The serialization was empty")
  }

  test("can deserialize exception") {

    val codec = new NrvProtosCodec()

    val bytes = codec.serializeToBytes(generateException())
    val exception = codec.serializeFromBytes(bytes)

    exception.asInstanceOf[Exception].getMessage should equal("/ by zero")
  }
}

