package com.wajam.nrv.protocol

import codec.{DummyCodec, GenericJavaSerializeCodec}
import com.wajam.nrv.data._
import com.wajam.nrv.service._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import com.wajam.nrv.cluster.{LocalNode, StaticClusterManager, Cluster}
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.scalatest.matchers.ShouldMatchers

@RunWith(classOf[JUnitRunner])
class TestNrvProtocolWithCluster extends FunSuite with BeforeAndAfter with ShouldMatchers {

  var cluster: Cluster = null
  var action: Action = null
  var notifyingProtocol: NrvProtocol = null

  val notifier = new Object()
  var received: Message = null

  before {
    cluster = new Cluster(new LocalNode("127.0.0.1", Map("nrv" -> 12345, "test" -> 12346)), new StaticClusterManager)

    action = new Action("dummy", (msg) => None,
      actionSupportOptions =  new ActionSupportOptions(dataBinaryCodec = Some("dummy", new GenericJavaSerializeCodec)))

    notifyingProtocol = new NrvProtocol(cluster.localNode) {
      override def handleIncoming(action: Action, message: InMessage) {
        received = message

        notifier.synchronized {
          notifier.notify()
        }
      }
    }
  }

  test("out-in") {

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

    // Trigger the message sending
    protocol.handleOutgoing(action, req)

    notifier.synchronized {
      notifier.wait(100)
    }

    assert(received != null)
    assert(received.parameters.getOrElse("test", "") === MString("someval"))

    cluster.stop()
  }

  test("test connection failure") {

    val protocol = notifyingProtocol

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

    val protocol = notifyingProtocol

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

    // This bind register the content type
    protocol.bindAction(actionDummy)

    // Trigger the message sending
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

  test("test multiple codec with same key failure")
  {
    val protocol = notifyingProtocol

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
