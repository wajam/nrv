package com.wajam.nrv.protocol

import org.scalatest.FunSuite
import com.wajam.nrv.cluster.{StaticClusterManager, Cluster, Node, LocalNode}
import com.wajam.nrv.data._
import org.mockito.Mockito._
import org.mockito.Matchers._
import org.scalatest.matchers.ShouldMatchers
import com.wajam.nrv.utils.test.PredicateMatcher
import org.scalatest.mock.MockitoSugar
import com.wajam.nrv.service._
import com.wajam.nrv.protocol.codec.GenericJavaSerializeCodec

class TestLocalOptimizedProtocol  extends FunSuite with ShouldMatchers with MockitoSugar {



  private def sendMessageAndVerifyCalls(ipSource: String,
                                        ipDst: String,
                                        portSource: Int,
                                        portDst: Int,
                                        shouldUseLocal: Boolean) {

    val node = new LocalNode(ipSource, Map("nrv" -> 1, "local" -> portSource))
    val destinationNode = new Node(ipDst, Map("nrv" -> 1, "local" -> portDst))

    val local = mock[Protocol]
    val remote = mock[Protocol]
    val action = mock[Action]

    // Ensure name constraint is respected
    doReturn("local").when(local).name
    doReturn("local").when(remote).name

    val lop = spy(new NrvLocalOptimizedProtocol("local", node, local, remote))

    lop.start()

    val message = spy(new OutMessage())
    val token = 100

    // Set the destination
    message.destination = new Endpoints(Seq(new Shard(token, Seq(new Replica(token, destinationNode, true)))))

    // Simulate real outgoing message
    lop.handleOutgoing(action, message)

    if (shouldUseLocal) {

      val flagMatcher = new PredicateMatcher((flags: Map[String, Any]) => flags.getOrElse("isLocalBound", false).asInstanceOf[Boolean] == shouldUseLocal)

      verify(local, timeout(100)).generate(anyObject(), argThat(flagMatcher))
      verify(local).sendMessage(anyObject(), anyObject(), anyBoolean(), argThat(flagMatcher), anyObject())
    }
    else {

      verify(remote, timeout(100)).generate(anyObject(), anyObject())
      verify(remote).sendMessage(anyObject(), anyObject(), anyBoolean(), anyObject(), anyObject())
    }
  }

  test("when out message to local node, send to the local protocol") {

    sendMessageAndVerifyCalls("127.0.0.1", "127.0.0.1", 12345, 12345, true)
  }

  test("when out message to remote node, send to the remote protocol") {

    sendMessageAndVerifyCalls("127.0.0.1", "8.8.8.8", 12345, 12345, false)
  }

  test("when out message to local node, on different port, send to the remote protocol") {

    // Can be another process!
    sendMessageAndVerifyCalls("127.0.0.1", "127.0.0.1", 12345, 12346, false)
  }

  test("can send-receive a local message in a real context (integration)") {

    // Test a complete roundtrip of a message with request-response.
    // It avoid using a cluster, switchboard, by hijacking callIncomingHandlers

    val node = new LocalNode("127.0.0.1", Map("nrv" -> 10000, "local" -> 12345))

    val cluster = new Cluster(node, new StaticClusterManager)

    val service = new Service("ServiceA")

    val action = new Action("ActionA", (msg) => None,
      actionSupportOptions =  new ActionSupportOptions(nrvCodec = Some( new GenericJavaSerializeCodec)))

    service.registerAction(action)

    val nrvProtocol = new NrvProtocol(cluster.localNode, 10000, 100)
    val localProtocol = spy(new NrvMemoryProtocol(nrvProtocol.name, node))
    val loProtocol = spy(new NrvLocalOptimizedProtocol(nrvProtocol.name, node, localProtocol, nrvProtocol))

    cluster.registerProtocol(loProtocol)

    cluster.start()

    val req = new OutMessage(Map("test" -> "someval"))
    req.destination = new Endpoints(Seq(new Shard(0, Seq(new Replica(0, cluster.localNode)))))

    // Simulate switchboard configuration
    req.source = cluster.localNode
    req.serviceName = service.name
    req.path = action.path.buildPath(req.parameters)

    loProtocol.bindAction(action)

    // Hijack handleIncoming to foward the response right away
    val routingAction = new Action("",
      (in) => Unit,
      ActionMethod.ANY,
      new ActionSupportOptions(cluster=Some(cluster))) {

      def generateFakeResponse(fromMessage: InMessage, outMessage: OutMessage) {
        outMessage.timestamp = fromMessage.timestamp
        outMessage.path = fromMessage.path
        outMessage.method = fromMessage.method
        outMessage.function = MessageType.FUNCTION_RESPONSE
        outMessage.rendezvousId = fromMessage.rendezvousId
        outMessage.attachments ++= fromMessage.attachments

        outMessage.destination = new Endpoints(Seq(new Shard(0, Seq(new Replica(0, fromMessage.source)))))
      }

      override def callIncomingHandlers(fromMessage: InMessage) {
        val outMessage = new OutMessage()

        // When is a request, generate response, else end call (normally callback would be called)
        if (fromMessage.function == MessageType.FUNCTION_CALL) {
          generateFakeResponse(fromMessage, outMessage)

          loProtocol.handleOutgoing(action, outMessage)
        }
      }
    }

    doReturn(Some(routingAction)).when(localProtocol).resolveAction(anyString(), anyString(), anyObject())

    // Trigger the message sending
    loProtocol.handleOutgoing(action, req)

    // Ensure the isLocalBound flags is set to ensure later routing of incoming message and of the future response

    val hasFlag =
      new PredicateMatcher((f: Map[String, Any]) =>
        f.getOrElse("isLocalBound", false).asInstanceOf[Boolean])

    val msgHasFlag =
      new PredicateMatcher((m: Message) =>
        hasFlag.matches(m.attachments(Protocol.FLAGS).asInstanceOf[Map[String, Any]]))


    Thread.sleep(100)

    // Verify the sequence of action required for a proper request-response call has happened. The order is NOT verified.
    // If you are familiar with Mockito and the fact verify are global,you can notice there is double checks,
    // but it's much more readable that way since it represent the real flow a message instead of compressed version

    // Request is sent
    verify(localProtocol, atLeast(1)).generate(anyObject(), argThat(hasFlag))
    verify(localProtocol, atMost(1)).sendMessage(anyObject(), anyObject(), anyBoolean(), argThat(hasFlag), anyObject())

    // Request is received
    verify(localProtocol, atLeast(1)).parse(anyObject(), argThat(hasFlag))
    verify(localProtocol, atLeast(1)).handleIncoming(anyObject(), argThat(msgHasFlag))

    // Fake-response is sent
    verify(loProtocol, atLeast(2)).handleOutgoing(anyObject(), anyObject())
    verify(localProtocol, atLeast(2)).generate(anyObject(), argThat(hasFlag))
    verify(localProtocol, atMost(1)).sendResponse(anyObject(), anyObject(), anyBoolean(), argThat(hasFlag), anyObject())

    // Response is received
    verify(localProtocol, atMost(2)).parse(anyObject(), argThat(hasFlag))
    verify(localProtocol, atMost(2)).handleIncoming(anyObject(), argThat(msgHasFlag))

    cluster.stop()
  }
}
