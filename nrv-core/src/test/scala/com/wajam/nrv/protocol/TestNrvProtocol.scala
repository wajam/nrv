package com.wajam.nrv.protocol

import codec.{DummyCodec, GenericJavaSerializeCodec, MessageJavaSerializeCodec}
import com.wajam.nrv.data._
import com.wajam.nrv.service._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import com.wajam.nrv.cluster.{LocalNode, StaticClusterManager, Cluster}
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.matchers.ShouldMatchers._

@RunWith(classOf[JUnitRunner])
class TestNrvProtocol extends FunSuite with BeforeAndAfter with ShouldMatchers {

  private def compareTwoProtocols(vA: NrvProtocolVersion.Value, vB: NrvProtocolVersion.Value) = {
    val message = new SerializableMessage()

    message.rendezvousId = 1234

    val nodeA: LocalNode = new LocalNode("127.0.0.1", Map("nrv" -> 12345))
    val nodeB: LocalNode = new LocalNode("127.0.0.1", Map("nrv" -> 12346))

    val protocolVA = new NrvProtocol(nodeA, protocolVersion = vA)
    val protocolVB = new NrvProtocol(nodeB, protocolVersion = vB)

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

@RunWith(classOf[JUnitRunner])
class TestNrvProtocolWithCluster extends FunSuite with BeforeAndAfter {

  var cluster: Cluster = null
  var action: Action = null

  before {
    cluster = new Cluster(new LocalNode("127.0.0.1", Map("nrv" -> 12345, "test" -> 12346)), new StaticClusterManager)
    action = new Action("dummy", (msg) => None,
      actionSupportOptions =  new ActionSupportOptions(dataBinaryCodec = Some("dummy", new GenericJavaSerializeCodec)))
  }

  test("out-in") {
    val notifier = new Object()
    var received: Message = null

    val protocol = new NrvProtocol(cluster.localNode) {

      override def parse(message: AnyRef): Message = {
        val parsedMsg = super.parse(message)
        received = parsedMsg

        notifier.synchronized {
          notifier.notify()
        }
        parsedMsg
      }
    }
    cluster.registerProtocol(protocol)

    cluster.start()

    val req = new OutMessage(Map("test" -> "someval"))
    req.destination = new Endpoints(Seq(new Shard(0, Seq(new Replica(0, cluster.localNode)))))
    protocol.handleOutgoing(action, req)

    notifier.synchronized {
      notifier.wait(100)
    }

    assert(received != null)
    assert(received.parameters.getOrElse("test", "") === MString("someval"))

    cluster.stop()
  }

  test("test connection failure") {

    val notifier = new Object()
    var received: Message = null
    val protocol = new NrvProtocol(cluster.localNode) {
      override def handleIncoming(action: Action, message: InMessage) {
        received = message

        notifier.synchronized {
          notifier.notify()
        }
      }
    }
    val req = new OutMessage(Map("test" -> "someval"))
    req.destination = new Endpoints(Seq(new Shard(0, Seq(new Replica(0, cluster.localNode)))))
    protocol.handleOutgoing(action, req)

    notifier.synchronized {
      notifier.wait(100)
    }

    assert(received.error != None)
  }

  test("test message reception failure") {

    val protocol = new NrvProtocol(cluster.localNode) {
      override def parse(message: AnyRef): Message = {
        throw new RuntimeException
      }

      override def handleIncoming(action: Action, message: InMessage) {
        fail("should not call handle incoming")
      }

      override def handleOutgoing(action: Action, message: OutMessage) {
        fail("should not call handle outgoing")
      }
    }

    protocol.transportMessageReceived("invalidmessage".getBytes, None)
  }

  test("test message parsing failure") {

    val protocol = new NrvProtocol(cluster.localNode) {
      override def parse(message: AnyRef): Message = {
        throw new ParsingException("400")
      }

      override def handleIncoming(action: Action, message: InMessage) {
        fail("should not call handle incoming")
      }

      override def handleOutgoing(action: Action, message: OutMessage) {
        fail("should not call handle outgoing")
      }
    }

    protocol.transportMessageReceived("invalidmessage".getBytes, None)
  }

  test("test contentType is setted and provided codec is used") {
    val notifier = new Object()
    var received: Message = null

    val protocol = new NrvProtocol(cluster.localNode) {
      override def handleIncoming(action: Action, message: InMessage) {
        received = message

        notifier.synchronized {
          notifier.notify()
        }
      }
    }

    cluster.registerProtocol(protocol)

    cluster.start()

    val req = new OutMessage(Map("test" -> "someval"))

    val codec = new DummyCodec

    val dummyService = new Service("test")

    val actionDummy = new Action("action", (msg) => None,
      actionSupportOptions =  new ActionSupportOptions(dataBinaryCodec = Some("dummy", codec))) {
      this._service = dummyService
    }

    req.destination = new Endpoints(Seq(new Shard(0, Seq(new Replica(0, cluster.localNode)))))

    protocol.bindAction(actionDummy)

    // The main action
    protocol.handleOutgoing(actionDummy, req)

    notifier.synchronized {
      notifier.wait(1000)
    }

    received.error should be(None)
    received.contentType == "dummy"
    codec.hasEncoded should be(true)
    //codec.hasDecoded should be(true)

    cluster.stop()
  }

  test("test multiple codec with same key failure") {
    val notifier = new Object()
    var received: Message = null

    val protocol = new NrvProtocol(cluster.localNode) {
      override def handleIncoming(action: Action, message: InMessage) {
        received = message

        notifier.synchronized {
          notifier.notify()
        }
      }
    }

    cluster.registerProtocol(protocol)

    cluster.start()

    val req = new OutMessage(Map("test" -> "someval"))

    val codec = new DummyCodec

    val dummyService = new Service("test")

    val actionDummy = new Action("action", (msg) => None,
      actionSupportOptions =  new ActionSupportOptions(dataBinaryCodec = Some("dummy", codec))) {
      this._service = dummyService
    }

    req.destination = new Endpoints(Seq(new Shard(0, Seq(new Replica(0, cluster.localNode)))))

    protocol.bindAction(actionDummy)

    intercept[UnsupportedOperationException] {
      // Oups dummy already exists!
      protocol.bindAction(action)
    }
  }

  after {
    cluster.stop()
  }
}