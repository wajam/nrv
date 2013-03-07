package com.wajam.nrv.protocol

import codec.{GenericJavaSerializeCodec, MessageJavaSerializeCodec}
import com.wajam.nrv.data._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import com.wajam.nrv.cluster.LocalNode
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.scalatest.matchers.ShouldMatchers
import serialization.NrvProtobufSerializer

@RunWith(classOf[JUnitRunner])
class TestNrvProtocol extends FunSuite with BeforeAndAfter with ShouldMatchers {

  class MockProtocol(localNode: LocalNode, protocolVersion: NrvProtocolVersion.Value = NrvProtocolVersion.V2)
    extends NrvProtocol(localNode, protocolVersion) {

    protected override val protobufSerializer = new NrvProtobufSerializer((Message) => new GenericJavaSerializeCodec)
  }

  private def compareTwoProtocols(vA: NrvProtocolVersion.Value, vB: NrvProtocolVersion.Value) = {
    val message = new SerializableMessage()

    message.rendezvousId = 1234

    val nodeA: LocalNode = new LocalNode("127.0.0.1", Map("nrv" -> 12345))
    val nodeB: LocalNode = new LocalNode("127.0.0.1", Map("nrv" -> 12346))

    val protocolVA = new MockProtocol(nodeA, protocolVersion = vA)

    val protocolVB = new MockProtocol(nodeB, protocolVersion = vB)

    val bytes = protocolVA.generate(message)
    val decodedMessage = protocolVB.parse(bytes)

    assert(message.rendezvousId === decodedMessage.rendezvousId)
  }

  test("can encode using v1 and can decode using v1") {
    compareTwoProtocols(NrvProtocolVersion.V1, NrvProtocolVersion.V2)
  }

  test("can encode using v1 and can decode using v2") {
    compareTwoProtocols(NrvProtocolVersion.V1, NrvProtocolVersion.V2)
  }

  test("can encode using v2 and can decode using v1") {
    compareTwoProtocols(NrvProtocolVersion.V2, NrvProtocolVersion.V1)
  }

  test("can encode using v2 and can decode using v2") {
    compareTwoProtocols(NrvProtocolVersion.V2, NrvProtocolVersion.V2)
  }

  test("encode w/ plain java serialization and can decode v1") {
    val message = new SerializableMessage()

    message.rendezvousId = 1234

    val nodeA: LocalNode = new LocalNode("127.0.0.1", Map("nrv" -> 12345))

    val protocolV1A = new NrvProtocol(nodeA, protocolVersion = NrvProtocolVersion.V1)

    val javaSerializer = new MessageJavaSerializeCodec()

    val bytes = javaSerializer.encode(message)
    val decodedMessage = protocolV1A.parse(bytes)

    assert(message.rendezvousId === decodedMessage.rendezvousId)
  }

  test("encode w/ v1 and decode plain java serialization") {
    val message = new SerializableMessage()

    message.rendezvousId = 1234

    val nodeA: LocalNode = new LocalNode("127.0.0.1", Map("nrv" -> 12345))

    val protocol = new NrvProtocol(nodeA, protocolVersion = NrvProtocolVersion.V1)

    val javaSerializer = new MessageJavaSerializeCodec()

    val bytes = protocol.generate(message).asInstanceOf[Array[Byte]]
    val decodedMessage = javaSerializer.decode(bytes).asInstanceOf[Message]

    assert(message.rendezvousId === decodedMessage.rendezvousId)
  }
}