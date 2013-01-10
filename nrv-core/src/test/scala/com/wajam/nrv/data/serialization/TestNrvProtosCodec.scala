package com.wajam.nrv.data.serialization

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FunSuite
import org.scalatest.matchers.ShouldMatchers._
import com.wajam.nrv.data.MessageType
import com.wajam.nrv.data.InMessage
import com.wajam.nrv.service.{Shard, Replica, Endpoints, ActionMethod}
import com.wajam.nrv.cluster.Node
import java.net.InetAddress
import com.wajam.nrv.protocol.codec.JavaSerializeCodec

@RunWith(classOf[JUnitRunner])
class TestNrvProtosCodec extends FunSuite {

  def getMessage() = {
    val message = new InMessage()

    message.protocolName = "Nrv"
    message.serviceName = "Nrv"
    message.method = ActionMethod.PUT
    message.path = "Yellow brick road"
    message.rendezvousId = 1024

    message.error = Some(generateException())

    message.function = MessageType.FUNCTION_CALL

    val localhost = InetAddress.getLocalHost()

    message.source = new Node(localhost, Map(("nrv", 1024)))

    val replica1 = new Replica(1024, new Node(localhost, Map(("nrv", 1025))))

    val shard1 = new Shard(1024, Seq(replica1))

    message.destination.shards :+ shard1

    message.token = 1024

    message.parameters += "Key" -> "Value"
    message.metadata += "CONTENT-TYPE" -> "text/plain"
    message.messageData = "Blob Of Data"

    // Here you go a message with possible value set (attachments is not serialized)

    message
  }

  test("can encode message") {
    val codec = new NrvProtosCodec()
    val messageDataCodec = new JavaSerializeCodec()

    val message1 = getMessage()

    val bytes = codec.encodeMessage(message1, messageDataCodec)

    // Not empty and did not crash
    assert(bytes != Array(Byte), "The serialization was empty")
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

