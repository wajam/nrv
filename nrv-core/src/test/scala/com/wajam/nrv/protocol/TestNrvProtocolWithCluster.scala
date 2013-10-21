package com.wajam.nrv.protocol

import codec.{DummyCodec, GenericJavaSerializeCodec}
import com.wajam.nrv.data._
import MessageType.FUNCTION_RESPONSE
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
  var service: Service = null
  var notifyingProtocol: NrvProtocol = null

  val notifier = new Object()
  var received: Message = null

  before {
    cluster = new Cluster(new LocalNode("127.0.0.1", Map("nrv" -> 12345, "test" -> 12346)), new StaticClusterManager)

    service = new Service("ServiceA")

    action = new Action("ActionA", (msg) => None,
      actionSupportOptions =  new ActionSupportOptions(nrvCodec = Some( new GenericJavaSerializeCodec)))

    service.registerAction(action)

    notifyingProtocol = new NrvProtocol(cluster.localNode, 10000, 100) {
      override def handleIncoming(action: Action, message: InMessage) {
        received = message

        notifier.synchronized {
          notifier.notify()
        }
      }
    }
  }

  after {
    cluster.stop()
  }

  test("out-in") {

    val protocol = new NrvProtocol(cluster.localNode, 10000, 100) {

      override def parse(message: AnyRef, flags: Map[String, Any]): Message = {
        val parsedMsg = super.parse(message, null)
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

    // Simulate switchboard configuration
    req.source = cluster.localNode
    req.serviceName = service.name
    req.path = action.path.buildPath(req.parameters)

    protocol.bindAction(action)

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

    // Simulate switchboard configuration
    req.source = cluster.localNode
    req.serviceName = service.name
    req.path = action.path.buildPath(req.parameters)

    protocol.bindAction(action)

    protocol.handleOutgoing(action, req)

    notifier.synchronized {
      notifier.wait(100)
    }

    assert(received.error != None)
  }

  test("test message reception failure") {

    val protocol = new NrvProtocol(cluster.localNode, 10000, 100) {
      override def parse(message: AnyRef, flags: Map[String, Any]): Message = {
        throw new RuntimeException
      }

      override def handleIncoming(action: Action, message: InMessage) {
        fail("should not call handle incoming")
      }

      override def handleOutgoing(action: Action, message: OutMessage) {
        fail("should not call handle outgoing")
      }
    }

    protocol.transportMessageReceived("invalidmessage".getBytes, None, Map())
  }

  test("test message parsing failure") {

    val protocol = new NrvProtocol(cluster.localNode, 10000, 100) {
      override def parse(message: AnyRef, flags: Map[String, Any]): Message = {
        throw new ParsingException("400", FUNCTION_RESPONSE)
      }

      override def handleIncoming(action: Action, message: InMessage) {
        fail("should not call handle incoming")
      }

      override def handleOutgoing(action: Action, message: OutMessage) {
        fail("should not call handle outgoing")
      }
    }

    protocol.transportMessageReceived("invalidmessage".getBytes, None, Map())
  }

  test("test overriden codec is used") {

    val protocol = notifyingProtocol

    cluster.registerProtocol(protocol)

    cluster.start()

    val req = new OutMessage(Map("test" -> "someval"))

    val codec = new DummyCodec

    val actionDummy = new Action("dummy", (msg) => None,
      actionSupportOptions =  new ActionSupportOptions(nrvCodec = Some(codec))) {
    }

    service.registerAction(actionDummy)

    req.destination = new Endpoints(Seq(new Shard(0, Seq(new Replica(0, cluster.localNode)))))

    // Simulate switchboard configuration
    req.source = cluster.localNode
    req.serviceName = service.name
    req.path = actionDummy.path.buildPath(req.parameters)

    protocol.bindAction(actionDummy)

    // Trigger the message sending
    protocol.handleOutgoing(actionDummy, req)

    notifier.synchronized {
      notifier.wait(1000)
    }

    received.error should be(None)

    codec.hasEncoded should be(true)
    codec.hasDecoded should be(true)

    cluster.stop()
  }
}
