package com.wajam.nrv.transport.netty

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite}
import com.wajam.nrv.protocol.Protocol
import org.jboss.netty.handler.codec.string.{StringEncoder, StringDecoder}
import org.jboss.netty.channel._
import com.wajam.nrv.data.{InMessage, Message}
import java.net.{InetSocketAddress, InetAddress}

@RunWith(classOf[JUnitRunner])
class TestNettyTransport extends FunSuite with BeforeAndAfter {

  val host = InetAddress.getByName("0.0.0.0")
  val port = 54321
  val notifier = new Object()

  var nettyTransport: NettyTransport = null
  var mockProtocol: MockProtocol = null

  object TestEncoderDecoderFactory extends NettyTransportCodecFactory {

    def createRequestEncoder() = encoder

    def createResponseEncoder() = encoder

    def createRequestDecoder() = decoder

    def createResponseDecoder() = decoder

    val encoder = new StringEncoder() {
      override def encode(ctx: ChannelHandlerContext, channel: Channel, msg: AnyRef) = {
        super.encode(ctx, channel, msg.toString)
      }
    }

    val decoder = new StringDecoder() {
      override def decode(ctx: ChannelHandlerContext, channel: Channel, msg: AnyRef) = {
        val message = new InMessage()
        message.loadData(Map("text" -> super.decode(ctx, channel, msg)))
        message
      }
    }
  }

  class MockProtocol extends Protocol("test", null) {
    var receivedMessage: String = null

    override def parse(message: AnyRef): Message = {
      receivedMessage = message.asInstanceOf[Message].getOrElse("text", "").asInstanceOf[String]
      notifier.synchronized {
        notifier.notify()
      }
      null
    }

    override def generate(message: Message): AnyRef = null

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
    nettyTransport.sendMessage(new InetSocketAddress("127.0.0.1", port), "hello")

    notifier.synchronized {
      notifier.wait(1000)
    }

    assert(mockProtocol.receivedMessage != null)
    assert(mockProtocol.receivedMessage.equals("hello"))
  }

  test("send message to invalid destination") {
    var result: AnyRef = null
    nettyTransport.sendMessage(new InetSocketAddress("127.0.0.1:", 12344), "hello", (opResult: Option[Throwable]) => {
      opResult match {
        case None => fail()
        case Some(t) => result = t
      }
    })

    assert(result.isInstanceOf[Throwable])
  }


}
