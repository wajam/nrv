package com.wajam.nrv.protocol

import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.scalatest.matchers.ShouldMatchers._
import com.wajam.nrv.cluster.{Node, Cluster}
import org.jboss.netty.handler.codec.http._

/**
 *
 */

@RunWith(classOf[JUnitRunner])
class TestHttpProtocol extends FunSuite with BeforeAndAfter {

  var protocol: HttpProtocol = null

  before {
    val node = new Node("localhost", Map("nrv" -> 19191, "test" -> 1909))
    protocol = new HttpProtocol("test", new Cluster(node, null))
  }

  test("should map HTTP method to message method") {
    val nettyRequest = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "")

    val msg = protocol.parse(nettyRequest)

    msg.method should equal ("GET")
  }

  test("should map HTTP path to message path") {
    val nettyRequest = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "path")

    val msg = protocol.parse(nettyRequest)

    msg.path should equal ("path")
  }

  test("should map HTTP query to message parameters") {
    val nettyRequest = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "path?a=1&b=2")

    val msg = protocol.parse(nettyRequest)

    msg.parameters.size should equal (2)
    msg.parameters("a") should equal (Seq("1"))
    msg.parameters("b") should equal (Seq("2"))
  }

  test("should map HTTP header to message metadata in requests") {
    val nettyRequest = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/")
    nettyRequest.addHeader("header", "value")

    val msg = protocol.parse(nettyRequest)

    msg.metadata.size should equal (1)
    msg.metadata("HEADER") should equal (Seq("value"))
  }

  test("should map HTTP header to message metadata in responses") {
    val nettyresponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK)
    nettyresponse.addHeader("header", "value")

    val msg = protocol.parse(nettyresponse)

    msg.metadata.size should equal (1)
    msg.metadata("HEADER") should equal (Seq("value"))
  }
}
