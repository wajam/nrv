package com.wajam.nrv.transport.netty

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite}
import java.net.InetAddress
import org.jboss.netty.channel.{Channel, ChannelHandlerContext}
import org.jboss.netty.handler.codec.string.{StringDecoder, StringEncoder}
import com.wajam.nrv.protocol.Protocol
import com.wajam.nrv.service.Action
import com.wajam.nrv.data.{Message, InRequest}
import org.jboss.netty.handler.codec.http._

/**
 * This class...
 *
 * User: felix
 * Date: 09/04/12
 */


@RunWith(classOf[JUnitRunner])
class TestHttpNettyTransport extends FunSuite with BeforeAndAfter {

  val host = InetAddress.getByName("0.0.0.0")
  val port = 54321
  val notifier = new Object()

  var nettyTransport :HttpNettyTransport = null
  var mockProtocol : MockProtocol = null

  class MockProtocol extends Protocol("test", null) {
    var receivedMessage : String = null

    def handleOutgoing(action: Action, message: Message) {}

    override def handleIncoming(message: AnyRef) {
      receivedMessage = message.asInstanceOf[HttpRequest].getUri()
      notifier.synchronized {
        notifier.notify()
      }
    }

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

  test ("send message to self") {
    val request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "uri")
    nettyTransport.sendMessage(host, port, request)

    notifier.synchronized {
      notifier.wait(100)
    }

    assert(mockProtocol.receivedMessage != null)
    assert(mockProtocol.receivedMessage.equals("uri"))
  }

}
