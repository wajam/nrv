package com.wajam.nrv.protocol

import org.scalatest.FunSuite
import com.wajam.nrv.cluster.{Node, LocalNode}
import com.wajam.nrv.data.{InMessage, Message, OutMessage}
import org.mockito.Mockito._
import org.mockito.Matchers._
import org.scalatest.matchers.ShouldMatchers
import com.wajam.nrv.utils.test.FunctionalMatcher
import org.scalatest.mock.MockitoSugar
import com.wajam.nrv.service.{Action, Endpoints, Replica, Shard}

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

      val flagMatcher = new FunctionalMatcher((flags: Map[String, Any]) => flags.getOrElse("isLocalBound", false).asInstanceOf[Boolean] == shouldUseLocal)

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
}
