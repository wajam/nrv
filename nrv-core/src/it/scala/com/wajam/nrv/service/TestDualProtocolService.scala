package com.wajam.nrv.service

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FunSuite
import com.wajam.nrv.cluster.{StaticClusterManager, LocalNode, Cluster}
import com.wajam.nrv.protocol.{NrvProtocol, HttpProtocol}
import com.wajam.nrv.data.{InMessage, MBoolean}
import scala.concurrent.{Future, Await}
import scala.concurrent.duration._

/**
 *
 */
@RunWith(classOf[JUnitRunner])
class TestDualProtocolService extends FunSuite {

  test("should support dual protocol interface for a single service") {
    val ports = Map("nrv" -> 5678, "http" -> 6789)
    val node = new LocalNode("127.0.0.1", ports)

    val httpProtocol = new HttpProtocol("http", node, 1000, 2)
    val nrvProtocol = new NrvProtocol(node, 1000, 2)

    var nrvCall = 0
    var httpCall = 0

    val service = new Service("test")
    val testAction = new Action("/path/", { inMessage =>
      inMessage.protocolName match {
        case "nrv" => nrvCall = nrvCall + 1
        case "http" => httpCall = httpCall + 1
      }
      inMessage.reply(Map(), data = "ack")
    })
    service.registerAction(testAction)
    service.applySupport(protocol = Some(nrvProtocol), supportedProtocols = Some(Set(httpProtocol)))

    val clusterManager = new StaticClusterManager
    clusterManager.addMembers(service, List("0:127.0.0.1:nrv=5678,http=6789"))

    val cluster = new Cluster(node, clusterManager)

    cluster.registerProtocol(httpProtocol)
    cluster.registerProtocol(nrvProtocol)
    cluster.registerService(service)
    cluster.start()

    def verify(responseFuture: Future[InMessage], protocolName: String, expectedNrvCalls: Int, expectedHttpCalls: Int) {
      val response = Await.result(responseFuture, 1 seconds)
      assert(nrvCall === expectedNrvCalls)
      assert(httpCall === expectedHttpCalls)
      assert(response.messageData === "ack")
      assert(response.protocolName === protocolName)
    }

    val nrvFuture = testAction.call(Map())
    verify(nrvFuture, "nrv", 1, 0)


    val httpFuture = testAction.call(Map(), Map(), null, 1000, protocolName = Some("http"))
    verify(httpFuture, "http", 1, 1)
  }


}
