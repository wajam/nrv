package com.wajam.nrv.transport.http

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.jboss.netty.handler.codec.http._
import java.net.{InetSocketAddress, InetAddress}
import com.wajam.nrv.service.Action
import com.wajam.nrv.data.{InMessage, Message}
import scala.Predef._
import com.wajam.nrv.protocol.Protocol

@RunWith(classOf[JUnitRunner])
class TestHttpNettyTransport extends FunSuite with BeforeAndAfter {

  val host = InetAddress.getByName("0.0.0.0")
  val port = 54322
  val notifier = new Object()

  var nettyTransport: HttpNettyTransport = null
  var mockProtocol: MockProtocol = null

  class MockProtocol extends Protocol("test") {
    override val transport = null
    var receivedURI: String = null

    override def parse(message: AnyRef): Message = {
      receivedURI = message.asInstanceOf[HttpRequest].getUri
      notifier.synchronized {
        notifier.notify()
      }
      null
    }


    override def handleIncoming(action: Action, message: InMessage) {

    }

    override def generate(message: Message): AnyRef = null

    override def start() {}

    override def stop() {}
  }

  before {
    mockProtocol = new MockProtocol
    nettyTransport = new HttpNettyTransport(host, port, mockProtocol)
    nettyTransport.start()
  }

  after {
    nettyTransport.stop()
  }

  test("send message to self") {
    val request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "uri")
    nettyTransport.sendMessage(new InetSocketAddress("127.0.0.1", port), request, true)

    notifier.synchronized {
      notifier.wait(100)
    }

    assert(mockProtocol.receivedURI != null)
    assert(mockProtocol.receivedURI.equals("uri"))
  }

  test("send message to self and response") {
    val request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "uri")
    nettyTransport.sendMessage(new InetSocketAddress("127.0.0.1", port), request, true)

    notifier.synchronized {
      notifier.wait(100)
    }

    assert(mockProtocol.receivedURI != null)
    assert(mockProtocol.receivedURI.equals("uri"))
  }

}
