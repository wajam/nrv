package com.wajam.nrv.data.serialization

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FunSuite
import org.scalatest.matchers.ShouldMatchers._
import com.wajam.nrv.data.MessageType
import com.wajam.nrv.data.Message
import com.wajam.nrv.data.InMessage
import com.wajam.nrv.service.{Shard, Replica, Endpoints, ActionMethod}
import com.wajam.nrv.cluster.Node
import java.net.InetAddress
import com.wajam.nrv.protocol.codec._

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

    val localhost = InetAddress.getLocalHost

    message.source = new Node(localhost, Map(("nrv", 1024)))

    val replica1 = new Replica(1024, new Node(localhost, Map(("nrv", 1025))))

    val shard1 = new Shard(1024, Seq(replica1))

    message.destination = new Endpoints(Seq(shard1))

    message.token = 1024

    message.metadata += "Key" -> "Value"
    message.metadata += "CONTENT-TYPE" -> "text/plain"
    message.messageData = "Blob Of Data"

    // Here you go a message with possible value set (attachments is not serialized)

    message
  }

  // TODO: Override .equals on every objects?

  def replicaIsEqual(replica1: Replica, replica2: Replica): Boolean = {
    (replica1.token == replica2.token) &&
    (nodeIsEqual(replica1.node, replica2.node)) &&
    (replica1.selected && replica2.selected)
  }

  def shardIsEqual(shard1: Shard, shard2: Shard): Boolean = {
    (shard1.token == shard2.token) &&
    shard1.replicas.forall( sd1 => shard2.replicas.exists(sd2 => replicaIsEqual(sd1, sd2)))
  }

  def endpointsAreEqual(endpoints1: Endpoints, endpoints2: Endpoints): Boolean = {
    endpoints1.shards.forall(sd1 => endpoints2.shards.exists(sd2 => shardIsEqual(sd1, sd2)))
  }

  def nodeIsEqual(node1: Node, node2: Node): Boolean = {
    (node1.host == node2.host) &&
    node1.ports.forall( kv1 => node2.ports.exists(kv2 => kv1._1 == kv2._1 && kv1._2 == kv1._2))
  }

  def messageIsEqual(message1: Message, message2: Message): Boolean = {
    val message = new InMessage()

    (message1.protocolName == message2.protocolName) &&
    (message1.serviceName == message2.serviceName) &&
    (message1.method == message2.method) &&
    (message1.path == message2.path) &&
    (message1.rendezvousId == message2.rendezvousId) &&
    (message1.error == message2.error) &&
    (message1.function == message2.function) &&
    (message1.token == message2.token) &&
    (message1.code == message2.code) &&
    nodeIsEqual(message1.source, message2.source) &&
    endpointsAreEqual(message1.destination, message2.destination) &&
    message1.parameters.forall( kv1 => message2.parameters.exists(kv2 => kv1._1 == kv2._1 && kv1._2 == kv1._2)) &&
    message1.metadata.forall( kv1 => message2.metadata.exists(kv2 => kv1._1 == kv2._1 && kv1._2 == kv1._2)) &&
    message1.messageData == message2.messageData
  }

  test("can encode/decode message") {
    val codec = new NrvProtosCodec()
    val messageDataCodec = new GenericJavaSerializeCodec()

    val message1 = getMessage()

    val bytes = codec.encodeMessage(message1, messageDataCodec)

    sys.error("not asserted yet")
  }

  test("can encode/decode node") {
    sys.error("unimplemented")
  }

  test("can encode/decode endpoints") {
    sys.error("unimplemented")
  }

  test("can encode/decode shards") {
    sys.error("unimplemented")
  }

  test("can encode/decode replica") {
    val codec = new NrvProtosCodec()

    val message = getMessage()
    val entity1 = message.destination.shards(0).replicas(0)

    val protoBufTransport = codec.encodeReplica(entity1)
    val entity2 = codec.decodeReplica(protoBufTransport)

    assert(entity1 == entity2, "The encode/decode failed, old and new entity are not the same")
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

