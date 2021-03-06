package com.wajam.nrv.protocol

import java.io.ByteArrayInputStream
import scala.language.reflectiveCalls
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.scalatest.Matchers._
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._
import org.mockito.Matchers.{anyObject, argThat, eq => mockEq}
import org.mockito.ArgumentMatcher
import org.jboss.netty.handler.codec.http._
import com.wajam.nrv.service._
import com.wajam.nrv.cluster.{LocalNode, StaticClusterManager, Cluster}
import com.wajam.nrv.data._
import com.wajam.nrv.data.MessageType._
import com.wajam.nrv.protocol.HttpProtocol.HttpChunkedResponse
import com.wajam.nrv.transport.http.HttpNettyTransport
import org.jboss.netty.channel.Channel

@RunWith(classOf[JUnitRunner])
class TestHttpProtocol extends FunSuite with BeforeAndAfter with MockitoSugar {
  val localnode = new LocalNode("localhost", Map("nrv" -> 19191, "http" -> 1909))
  val destination = new Endpoints(Seq(new Shard(0, Seq(new Replica(0, localnode, true)))))
  var unchunkedProtocol: HttpProtocol = null
  var chunkedProtocol: HttpProtocol = null

  val chunkSize = 1

  val fixture = {
    new {
      val sampleJsonData =
        """
          |{"name":"Load-edge_user1","activated":true,"referral_id":0,"email":"load-edge_user1@wajam.com","unique_id":"","remote_addr":"172.22.2.89","facebook_account_id":"0","email_send_reminder":true,"addon_version":"c1.24","user_mapping_version":179,"twitter_account_id":"0","user_id":43,"location_id":0,"last_login_date":"0000-00-00 00:00:00","signup_completed":false,"signup_date":"2013-06-11 09:19:32","profile_image":"5d295ffbff964d2ddb7de7fb4018ebc5.png","salt":"f9b0d1257aab","platform":"Win32","last_search_date":"0000-00-00 00:00:00","password":""}
        """.stripMargin
    }
  }

  before {
    val cluster = new Cluster(localnode, new StaticClusterManager)
    unchunkedProtocol = new HttpProtocol("http", localnode, 10000, 100)
    chunkedProtocol = new HttpProtocol("http", localnode, 10000, 100, Some(chunkSize))
  }

  /**
   * Tests to be run with and without chunking enabled
   */

  def commonHttpTests(protocol: => HttpProtocol, displayName: String) {

    test("should map HTTP query to message parameters (" + displayName + ")") {
      val nettyRequest = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "path?a=1&b=2&b=3")

      val msg = protocol.parse(nettyRequest, null)

      msg.parameters.size should equal(2)
      msg.parameters("a") should equal(MString("1"))
      msg.parameters("b") should equal(MList(Seq("2", "3")))
    }

    test("should map HTTP header to message metadata in requests (" + displayName + ")") {
      val nettyRequest = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/")
      nettyRequest.addHeader("header", "value")

      val msg = protocol.parse(nettyRequest, null)

      msg.metadata.size should equal(1)
      msg.metadata("HEADER") should equal(MString("value"))
    }

    test("should map HTTP header to message metadata in responses (" + displayName + ")") {
      val nettyresponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK)
      nettyresponse.addHeader("header", "value")

      val msg = protocol.parse(nettyresponse, null)

      msg.metadata("HEADER") should equal(MString("value"))
    }

    test("should HTTP map status code to code (" + displayName + ")") {
      val nettyresponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK)

      val msg = protocol.parse(nettyresponse, null)

      msg.code should equal(200)
    }

    test("should map status code in special header to code (" + displayName + ")") {
      val nettyRequest = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "")
      nettyRequest.addHeader(HttpProtocol.CODE_HEADER, 200)

      val msg = protocol.parse(nettyRequest, null)

      msg.code should equal(200)
    }

    test("should set code to HTTP request special header (" + displayName + ")") {
      val msg = new InMessage()
      msg.method = "GET"
      msg.code = 333
      msg.destination = destination

      val req = protocol.generate(msg).asInstanceOf[HttpRequest]

      assert(333 === req.getHeader(HttpProtocol.CODE_HEADER).toInt)
    }

    test("should use message code as status code (" + displayName + ")") {
      val msg = new OutMessage()
      msg.code = 500
      msg.function = MessageType.FUNCTION_RESPONSE

      val res = protocol.generate(msg) match {
        case chunkedResponse: HttpProtocol.HttpChunkedResponse => chunkedResponse.begin
        case default:DefaultHttpResponse => default
        case _ => fail("Invalid response")
      }

      assert(500 === res.getStatus.getCode)
    }

    test("should map HTTP method to message method (" + displayName + ")") {
      val nettyRequest = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "")

      val msg = protocol.parse(nettyRequest, null)

      msg.method should equal(ActionMethod("GET"))
    }

    test("should map special method HTTP header to message method (" + displayName + ")") {
      val nettyresponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK)
      nettyresponse.addHeader(HttpProtocol.METHOD_HEADER, ActionMethod.GET)

      val msg = protocol.parse(nettyresponse, null)

      msg.method should equal(ActionMethod.GET)
    }

    test("should set method in special HTTP header on response (" + displayName + ")") {
      val msg = new OutMessage()
      msg.method = ActionMethod.GET
      msg.function = MessageType.FUNCTION_RESPONSE

      val res = protocol.generate(msg) match {
        case chunkedResponse: HttpChunkedResponse => chunkedResponse.begin
        case default:DefaultHttpResponse => default
        case _ => fail("Invalid response")
      }

      assert("GET" === res.getHeader(HttpProtocol.METHOD_HEADER))
    }

    test("should set method in HTTP request (" + displayName + ")") {
      val msg = new InMessage()
      msg.method = ActionMethod.GET
      msg.destination = destination

      val req = protocol.generate(msg).asInstanceOf[HttpRequest]

      assert("GET" === req.getMethod.toString)
    }

    test("should map HTTP path to message path (" + displayName + ")") {
      val nettyRequest = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "path")

      val msg = protocol.parse(nettyRequest, null)

      msg.path should equal("path")
    }

    test("should map special method HTTP header to message path (" + displayName + ")") {
      val nettyresponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK)
      nettyresponse.addHeader(HttpProtocol.PATH_HEADER, "path")

      val msg = protocol.parse(nettyresponse, null)

      msg.path should equal("path")
    }

    test("should set path in special HTTP header on response (" + displayName + ")") {
      val msg = new OutMessage()
      msg.path = "/path"
      msg.function = MessageType.FUNCTION_RESPONSE

      val res = protocol.generate(msg) match {
        case chunkedResponse: HttpChunkedResponse => chunkedResponse.begin
        case default:DefaultHttpResponse => default
        case _ => fail("Invalid response")
      }

      assert("/path" === res.getHeader(HttpProtocol.PATH_HEADER))
    }

    test("should set path in HTTP request (" + displayName + ")") {
      val msg = new InMessage()
      msg.method = ActionMethod.GET
      msg.destination = destination
      msg.path = "/path"

      val req = protocol.generate(msg).asInstanceOf[HttpRequest]

      assert("/path" === req.getUri)
    }

    test("should generate and parse a complete message (" + displayName + ")") {
      val msg = new InMessage()
      msg.method = ActionMethod.GET
      msg.path = "/path"
      msg.destination = destination

      msg.metadata("CONTENT-TYPE") = MString("text/plain")

      val req = protocol.generate(msg).asInstanceOf[HttpRequest]
      val msg2 = protocol.parse(req, null)

      assert(msg.path === msg2.path)
      assert(msg.method === msg2.method)
      assert(msg.metadata("CONTENT-TYPE") === msg2.metadata("CONTENT-TYPE"))
    }
  }

  commonHttpTests(unchunkedProtocol, "without chunk")
  commonHttpTests(chunkedProtocol, "with chunk")

  /**
   * Specific tests for chunking
   */

  test("should set the correct Transfer-Encoding header") {
    val msg = new OutMessage()
    msg.code = 200
    msg.messageData = fixture.sampleJsonData
    msg.function = MessageType.FUNCTION_RESPONSE
    msg.destination = destination

    chunkedProtocol.generate(msg) match {
      case chunkedResponse: HttpChunkedResponse => {
        assert(chunkedResponse.begin.getHeader("Transfer-Encoding") === "chunked")
      }
      case _ => fail("Response was not chunked")
    }
  }

  test("should split the response in several chunks of defined size") {
    val msg = new OutMessage()
    msg.code = 200
    msg.messageData = fixture.sampleJsonData
    msg.function = MessageType.FUNCTION_RESPONSE
    msg.destination = destination

    chunkedProtocol.generate(msg) match {
      case chunkedResponse: HttpChunkedResponse => {
        assert(chunkedResponse.input.hasNextChunk)

        assert(chunkedResponse.input.nextChunk().asInstanceOf[HttpChunk].getContent.array().size  === chunkSize)
      }
      case _ => fail("Response was not chunked")
    }
  }

  /**
   * Tests for streaming
   */

  test("should generate a HttpChunkedResponse from an InputStream") {
    val dataStream = new ByteArrayInputStream(fixture.sampleJsonData.getBytes)
    val msg = new OutMessage(code = 200, data = dataStream)
    msg.function = MessageType.FUNCTION_RESPONSE

    assert(unchunkedProtocol.generate(msg).isInstanceOf[HttpChunkedResponse])
  }

  /**
   * Tests for error handling
   */

  // Check the status code of a HTTP response
  class IsResponseWithCode(code: Int) extends ArgumentMatcher {
    def matches(response: Any) = {
      response match {
        case res: HttpResponse =>
          res.getStatus.getCode == code
        case _ =>
          false
      }
    }
  }

  test("should send a 404 response when an incoming function call doesn't match any route") {
    val transportMock = mock[HttpNettyTransport]
    val protocol = new HttpProtocol("http", localnode, 10000, 100) {
      override val transport = transportMock
    }

    val msg = new InMessage()
    msg.function = FUNCTION_CALL
    msg.attachments += (Protocol.CONNECTION_KEY -> Some(mock[AnyRef]))
    msg.serviceName = "nonexistent"
    msg.path = "/nonexistent"

    protocol.handleIncoming(null, msg)

    verify(transportMock).sendResponse(anyObject(), argThat(new IsResponseWithCode(404)), mockEq(true), anyObject())
  }

  test("should close the connection without sending a response when an incoming function response doesn't match any route") {
    val transportMock = mock[HttpNettyTransport]
    val channelMock = Some(mock[Channel])
    val protocol = new HttpProtocol("http", localnode, 10000, 100) {
      override val transport = transportMock
    }

    val msg = new InMessage()
    msg.function = FUNCTION_RESPONSE
    msg.attachments += (Protocol.CONNECTION_KEY -> channelMock)
    msg.serviceName = "nonexistent"
    msg.path = "/nonexistent"

    protocol.handleIncoming(null, msg)

    verify(transportMock, never()).sendResponse(anyObject(), anyObject(), anyObject(), anyObject())
    verify(transportMock).closeChannel(channelMock)
  }

  test("should send a 406 response when unable to parse an incoming function call") {
    val serviceName = "dummy-service"
    val path = "/dummy-path"

    val dummyService = mock[Service]
    val dummyAction  = mock[Action]

    val request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, serviceName + path)
    // Voluntarily set an unknown content type
    request.setHeader("CONTENT-TYPE", "unknown/unknown")


    // Bind action to mocked service to avoid throwing RouteException
    when(dummyService.findAction(anyObject(), anyObject())).thenReturn(Some(dummyAction))

    val transportMock = mock[HttpNettyTransport]
    val protocol = new HttpProtocol("http", localnode, 10000, 100) {
      override val transport = transportMock
    }

    protocol.services += (serviceName -> dummyService)

    // Inject the request as if it came from transport, connectionInfo is not important
    protocol.transportMessageReceived(request, Some(mock[AnyRef]), Map[String, Any]())

    verify(transportMock).sendResponse(anyObject(), argThat(new IsResponseWithCode(406)), mockEq(true), anyObject())
  }

  test("should close the connection without sending a response when unable to parse an incoming function response") {
    val serviceName = "dummy-service"

    val dummyService = mock[Service]
    val dummyAction  = mock[Action]

    val channelMock = Some(mock[Channel])

    val response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK)
    // Voluntarily set an unknown content type
    response.setHeader("CONTENT-TYPE", "unknown/unknown")

    // Bind action to mocked service to avoid throwing RouteException
    when(dummyService.findAction(anyObject(), anyObject())).thenReturn(Some(dummyAction))

    val transportMock = mock[HttpNettyTransport]
    val protocol = new HttpProtocol("http", localnode, 10000, 100) {
      override val transport = transportMock
    }

    protocol.services += (serviceName -> dummyService)

    // Inject the response as if it came from transport, connectionInfo is not important
    protocol.transportMessageReceived(response, channelMock, Map[String, Any]())

    verify(transportMock, never()).sendResponse(anyObject(), anyObject(), anyObject(), anyObject())
    verify(transportMock).closeChannel(channelMock)
  }

  test("should close the connection without sending a response when a generic exception is thrown") {
    val serviceName = "dummy-service"
    val path = "/dummy-path"

    val channelMock = Some(mock[Channel])

    val request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, serviceName + path) {
      // Will throw a generic exception at the very beginning of parsing
      override def getMethod: HttpMethod = throw new Exception
    }
    request.setHeader("CONTENT-TYPE", "unknown/unknown")

    val transportMock = mock[HttpNettyTransport]
    val protocol = new HttpProtocol("http", localnode, 10000, 100) {
      override val transport = transportMock
    }

    // Inject the request as if it came from transport, connectionInfo is not important
    protocol.transportMessageReceived(request, channelMock, Map[String, Any]())

    verify(transportMock, never()).sendResponse(anyObject(), anyObject(), anyObject(), anyObject())
    verify(transportMock).closeChannel(channelMock)
  }
}
