package com.wajam.nrv.data.serialization

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{TestFailedException, FunSuite}
import org.scalatest.matchers.ShouldMatchers._
import com.wajam.nrv.data.{SerializableMessage, MessageType, Message, InMessage}
import com.wajam.nrv.service.{Shard, Replica, Endpoints, ActionMethod}
import com.wajam.nrv.cluster.Node
import java.net.InetAddress
import com.wajam.nrv.protocol.codec._

@RunWith(classOf[JUnitRunner])
class TestNrvProtobufSerializer extends FunSuite {

  private def makeMessage() = {
    val message = new SerializableMessage()

    message.protocolName = "Nrv"
    message.serviceName = "NrvService"
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

    message.parametersNew += "Key" -> Seq("Value")
    message.metadataNew += "CONTENT-TYPE" -> Seq("text/plain")
    message.messageData = "Blob Of Data"

    // Here you go a message with all possible value set (attachments is not serialized)

    message
  }

  private def replicaIsEqual(replica1: Replica, replica2: Replica): Boolean = {
    replica1.token == replica2.token
    nodeIsEqual(replica1.node, replica2.node)
    replica1.selected == replica2.selected
  }

  private def shardIsEqual(shard1: Shard, shard2: Shard): Boolean = {
    assert(shard1.token == shard2.token)
    shard1.replicas.forall(sd1 => shard2.replicas.exists(sd2 => replicaIsEqual(sd1, sd2)))
  }

  private def endpointsAreEqual(endpoints1: Endpoints, endpoints2: Endpoints): Boolean = {
    endpoints1.shards.forall(sd1 => endpoints2.shards.exists(sd2 => shardIsEqual(sd1, sd2)))
  }

  private def nodeIsEqual(node1: Node, node2: Node): Boolean = {

    (node1 == null && node2 == null) ||
    (node1.host == node2.host) &&
    node1.ports.forall( kv1 => node2.ports.exists(kv2 => kv1._1 == kv2._1 && kv1._2 == kv1._2))
  }

  private def exceptionIsEqual(ex1: Option[Exception], ex2: Option[Exception]): Boolean = {

    if (ex1.isEmpty && ex2.isEmpty)
    {
      true
    }
    else if (ex1.isDefined && ex2.isDefined)  {

      // Is this all we need?
      (ex1.get.getMessage == ex2.get.getMessage) &&
      (ex1.get.getStackTraceString == ex2.get.getStackTraceString)
    }
    else
    {
      false
    }
  }

  private def assertMessageEqual(message1: Message, message2: Message) = {

    assert(message1.protocolName === message2.protocolName)
    assert(message1.serviceName === message2.serviceName)
    assert(message1.method === message2.method)
    assert(message1.path === message2.path)
    assert(message1.rendezvousId === message2.rendezvousId)
    assert(exceptionIsEqual(message1.error, message2.error))
    assert(message1.function === message2.function)
    assert(message1.token === message2.token)
    assert(message1.code === message2.code)
    assert(nodeIsEqual(message1.source, message2.source))
    assert(endpointsAreEqual(message1.destination, message2.destination))
    assert(message1.parameters.forall( kv1 => message2.parameters.exists(kv2 => kv1._1 == kv2._1 && kv1._2 == kv2._2)))
    assert(message1.metadata.forall( kv1 => message2.metadata.exists(kv2 => kv1._1 == kv2._1 && kv1._2 == kv2._2)))
    assert(message1.parametersNew.forall( kv1 => message2.parametersNew.exists(kv2 => kv1._1 == kv2._1 && kv1._2 == kv2._2)))
    assert(message1.metadataNew.forall( kv1 => message2.metadataNew.exists(kv2 => kv1._1 == kv2._1 && kv1._2 == kv2._2)))
    assert(message1.messageData === message2.messageData)
  }

  test("can serialize/deserialize message") {
    val codec = new NrvProtobufSerializer()
    val messageDataCodec = new GenericJavaSerializeCodec()

    val entity1 = makeMessage()

    val contents = codec.serializeMessage(entity1, messageDataCodec)
    val entity2 = codec.deserializeMessage(contents, messageDataCodec)

    assertMessageEqual(entity1, entity2)
  }

  test("can encode/decode message") {
    val codec = new NrvProtobufSerializer()
    val messageDataCodec = new GenericJavaSerializeCodec()

    val entity1 = makeMessage()

    val protoBufTransport = codec.encodeMessage(entity1, messageDataCodec)
    val entity2 = codec.decodeMessage(protoBufTransport, messageDataCodec)

    assertMessageEqual(entity1, entity2)
  }

  ignore("can handle Any in parameters and metadata") {
    val codec = new NrvProtobufSerializer()
    val messageDataCodec = new GenericJavaSerializeCodec()

    val entity1 = makeMessage()

    entity1.metadata += "Key1m" -> 1
    entity1.parameters += "Key1p" -> 2

    entity1.metadata += "Key2m" -> 1.0
    entity1.parameters += "Key2p" -> 2.0

    entity1.metadata += "Key3m" -> new Pair("A", 1)
    entity1.parameters += "Key3p" -> new Pair("B", 2)

    val protoBufTransport = codec.encodeMessage(entity1, messageDataCodec)
    val entity2 = codec.decodeMessage(protoBufTransport, messageDataCodec)

    assertMessageEqual(entity1, entity2)
  }

  ignore("can drop Any in parameters and metadata") {
    val codec = new NrvProtobufSerializer(dropAny = true)
    val messageDataCodec = new GenericJavaSerializeCodec()

    val entity1 = makeMessage()

    entity1.metadata += "Key1m" -> 1
    entity1.parameters += "Key1p" -> 2

    val protoBufTransport = codec.encodeMessage(entity1, messageDataCodec)
    val entity2 = codec.decodeMessage(protoBufTransport, messageDataCodec)

    intercept[TestFailedException] {
      assertMessageEqual(entity1, entity2)
    }
  }

  ignore("can drop String in parameters and metadata") {
    val codec = new NrvProtobufSerializer(dropString = true)
    val messageDataCodec = new GenericJavaSerializeCodec()

    val entity1 = makeMessage()

    entity1.metadataNew += "Key1m" -> Seq("1")
    entity1.parametersNew += "Key1p" -> Seq("2")

    val protoBufTransport = codec.encodeMessage(entity1, messageDataCodec)
    val entity2 = codec.decodeMessage(protoBufTransport, messageDataCodec)

    intercept[TestFailedException] {
      assertMessageEqual(entity1, entity2)
    }
  }

  test("can encode/decode empty message") {
    val codec = new NrvProtobufSerializer()
    val messageDataCodec = new GenericJavaSerializeCodec()

    val entity1 = new SerializableMessage()

    val protoBufTransport = codec.encodeMessage(entity1, messageDataCodec)
    val entity2 = codec.decodeMessage(protoBufTransport, messageDataCodec)

    assertMessageEqual(entity1, entity2)
  }

  test("can encode/decode a message with no error in it") {
    val codec = new NrvProtobufSerializer()
    val messageDataCodec = new GenericJavaSerializeCodec()

    val entity1 = makeMessage()

    entity1.error = None

    val protoBufTransport = codec.encodeMessage(entity1, messageDataCodec)
    val entity2 = codec.decodeMessage(protoBufTransport, messageDataCodec)

    assertMessageEqual(entity1, entity2)
  }

  test("can encode/decode node") {
    val codec = new NrvProtobufSerializer()

    val message = makeMessage()
    val entity1 = message.source

    val protoBufTransport = codec.encodeNode(entity1)
    val entity2 = codec.decodeNode(protoBufTransport)

    assert(nodeIsEqual(entity1, entity2), "The encode/decode failed, old and new entity are not the same")
  }

  test("can encode/decode endpoints") {
    val codec = new NrvProtobufSerializer()

    val message = makeMessage()
    val entity1 = message.destination

    val protoBufTransport = codec.encodeEndpoints(entity1)
    val entity2 = codec.decodeEndpoints(protoBufTransport)

    endpointsAreEqual(entity1, entity2)
  }

  test("can encode/decode shards") {
    val codec = new NrvProtobufSerializer()

    val message = makeMessage()
    val entity1 = message.destination.shards(0)

    val protoBufTransport = codec.encodeShard(entity1)
    val entity2 = codec.decodeShard(protoBufTransport)

    shardIsEqual(entity1, entity2)
  }

  test("can encode/decode replica") {
    val codec = new NrvProtobufSerializer()

    val message = makeMessage()
    val entity1 = message.destination.shards(0).replicas(0)

    val protoBufTransport = codec.encodeReplica(entity1)
    val entity2 = codec.decodeReplica(protoBufTransport)

    replicaIsEqual(entity1, entity2)
  }

  def generateException() = {

    try {
      val x, y = 0
      x / y
      null
    }
    catch {
      case ex:Exception => ex
    }
  }

  test("can serialize exception") {

    val codec = new NrvProtobufSerializer()

    val bytes = codec.serializeToBytes(generateException())

    assert(bytes != Array(Byte), "The serialization was empty")
  }

  test("can deserialize exception") {

    val codec = new NrvProtobufSerializer()

    val bytes = codec.serializeToBytes(generateException())
    val exception = codec.serializeFromBytes(bytes)

    exception.asInstanceOf[Exception].getMessage should equal("/ by zero")
  }
}

