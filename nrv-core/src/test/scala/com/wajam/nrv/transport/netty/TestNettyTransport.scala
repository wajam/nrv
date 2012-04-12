package com.wajam.nrv.transport.netty

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite}
import java.net.InetAddress
import com.wajam.nrv.protocol.Protocol
import org.jboss.netty.handler.codec.string.{StringEncoder, StringDecoder}
import com.wajam.nrv.service.Action
import org.jboss.netty.channel._
import com.wajam.nrv.data.{InRequest, Message}
import com.wajam.nrv.utils.CompletionCallback


/**
 * This class...
 *
 * User: felix
 * Date: 05/04/12
 */

@RunWith(classOf[JUnitRunner])
class TestNettyTransport extends FunSuite with BeforeAndAfter {

  val host = InetAddress.getByName("0.0.0.0")
  val port = 54321
  val notifier = new Object()

  var nettyTransport: NettyTransport = null
  var mockProtocol: MockProtocol = null

  object TestEncoderDecoderFactory extends NettyTransportCodecFactory {
    override def createEncoder() = new StringEncoder() {
      override def encode(ctx: ChannelHandlerContext, channel: Channel, msg: AnyRef) = {
        super.encode(ctx, channel, msg.toString)
      }
    }

    override def createDecoder() = new StringDecoder() {
      override def decode(ctx: ChannelHandlerContext, channel: Channel, msg: AnyRef) = {
        val request = new InRequest()
        request.loadData(Map("text" -> super.decode(ctx, channel, msg)))
        request
      }
    }
  }

  class MockProtocol extends Protocol("test", null) {
    var receivedMessage: String = null

    def handleOutgoing(action: Action, message: Message) {}

    override def handleMessageFromTransport(message: AnyRef) {
      receivedMessage = message.asInstanceOf[Message].getOrElse("text", "").asInstanceOf[String]
      notifier.synchronized {
        notifier.notify()
      }
    }

    override def start() {}

    override def stop() {}
  }

  before {
    mockProtocol = new MockProtocol
    nettyTransport = new NettyTransport(host, port, mockProtocol, TestEncoderDecoderFactory)
    nettyTransport.start()
  }

  after {
    nettyTransport.stop()
  }

  test("send message to self") {
    nettyTransport.sendMessage(host, port, "hello")

    notifier.synchronized {
      notifier.wait(100)
    }

    assert(mockProtocol.receivedMessage != null)
    assert(mockProtocol.receivedMessage.equals("hello"))
  }

  test("send message to invalid destination") {
    var result: AnyRef = null
    nettyTransport.sendMessage(host, 8765, "hello", Some(new CompletionCallback {
      def operationCompleted(opResult: Option[Throwable]) {
        {
          opResult match {
            case None => fail()
            case Some(t) => result = t
          }
        }
      }
    }))

    assert(result.isInstanceOf[Throwable])
  }


}
