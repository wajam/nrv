package com.wajam.nrv.protocol

import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.scalatest.matchers.ShouldMatchers._
import com.wajam.nrv.cluster.{LocalNode, StaticClusterManager, Cluster}
import org.jboss.netty.handler.codec.http._
import com.wajam.nrv.service.ActionMethod
import com.wajam.nrv.data._
import com.wajam.nrv.data.MString

/**
 *
 */

@RunWith(classOf[JUnitRunner])
class TestHttpProtocol extends FunSuite with BeforeAndAfter {

  var protocol: HttpProtocol = null

  before {
    val localnode = new LocalNode("localhost", Map("nrv" -> 19191, "test" -> 1909))
    val cluster = new Cluster(localnode, new StaticClusterManager)
    protocol = new HttpProtocol("test", localnode, 10000, 100)
  }

  test("should map HTTP query to message parameters") {
    val nettyRequest = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "path?a=1&b=2&b=3")

    val msg = protocol.parse(nettyRequest)

    msg.parameters.size should equal(2)
    msg.parameters("a") should equal(MString("1"))
    msg.parameters("b") should equal(MList(Seq("2", "3")))
  }

  test("should map HTTP header to message metadata in requests") {
    val nettyRequest = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/")
    nettyRequest.addHeader("header", "value")

    val msg = protocol.parse(nettyRequest)

    msg.metadata.size should equal(1)
    msg.metadata("HEADER") should equal(MString("value"))
  }

  test("should map HTTP header to message metadata in responses") {
    val nettyresponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK)
    nettyresponse.addHeader("header", "value")

    val msg = protocol.parse(nettyresponse)

    msg.metadata("HEADER") should equal(MString("value"))
  }

  test("should HTTP map status code to code") {
    val nettyresponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK)

    val msg = protocol.parse(nettyresponse)

    msg.code should equal(200)
  }

  test("should map status code in special header to code") {
    val nettyRequest = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "")
    nettyRequest.addHeader(HttpProtocol.CODE_HEADER, 200)

    val msg = protocol.parse(nettyRequest)

    msg.code should equal(200)
  }

  test("should set code to HTTP request special header") {
    val msg = new InMessage()
    msg.method = "GET"
    msg.code = 333

    val req = protocol.generate(msg).asInstanceOf[HttpRequest]

    assert(333 === req.getHeader(HttpProtocol.CODE_HEADER).toInt)
  }

  test("should use message code as status code") {
    val msg = new OutMessage()
    msg.code = 500

    val res = protocol.generate(msg).asInstanceOf[HttpResponse]

    assert(500 === res.getStatus.getCode)
  }

  test("should map HTTP method to message method") {
    val nettyRequest = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "")

    val msg = protocol.parse(nettyRequest)

    msg.method should equal(ActionMethod("GET"))
  }

  test("should map special method HTTP header to message method") {
    val nettyresponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK)
    nettyresponse.addHeader(HttpProtocol.METHOD_HEADER, ActionMethod.GET)

    val msg = protocol.parse(nettyresponse)

    msg.method should equal(ActionMethod.GET)
  }

  test("should set method in special HTTP header on response") {
    val msg = new OutMessage()
    msg.method = ActionMethod.GET

    val res = protocol.generate(msg).asInstanceOf[HttpResponse]

    assert("GET" === res.getHeader(HttpProtocol.METHOD_HEADER))
  }

  test("should set method in HTTP request") {
    val msg = new InMessage()
    msg.method = ActionMethod.GET

    val req = protocol.generate(msg).asInstanceOf[HttpRequest]

    assert("GET" === req.getMethod.toString)
  }

  test("should map HTTP path to message path") {
    val nettyRequest = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "path")

    val msg = protocol.parse(nettyRequest)

    msg.path should equal("path")
  }

  test("should map special method HTTP header to message path") {
    val nettyresponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK)
    nettyresponse.addHeader(HttpProtocol.PATH_HEADER, "path")

    val msg = protocol.parse(nettyresponse)

    msg.path should equal("path")
  }

  test("should set path in special HTTP header on response") {
    val msg = new OutMessage()
    msg.path = "path"

    val res = protocol.generate(msg).asInstanceOf[HttpResponse]

    assert("path" === res.getHeader(HttpProtocol.PATH_HEADER))
  }

  test("should set path in HTTP request") {
    val msg = new InMessage()
    msg.method = ActionMethod.GET
    msg.path = "path"

    val req = protocol.generate(msg).asInstanceOf[HttpRequest]

    assert("path" === req.getUri)
  }

  test("should generate and parse a complete message") {
    val msg = new InMessage()
    msg.method = ActionMethod.GET
    msg.path = "path"

    msg.metadata("CONTENT-TYPE") = MString("text/plain")

    val req = protocol.generate(msg).asInstanceOf[HttpRequest]
    val msg2 = protocol.parse(req)

    assert(msg.path === msg2.path)
    assert(msg.method === msg2.method)
    assert(msg.metadata("CONTENT-TYPE") === msg2.metadata("CONTENT-TYPE"))
  }
}
