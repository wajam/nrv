package com.wajam.nrv.transport.netty

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.jboss.netty.handler.codec.string.{StringEncoder, StringDecoder}
import org.jboss.netty.channel._
import java.net.{InetSocketAddress, InetAddress}
import com.wajam.nrv.data.{InMessage, Message}
import com.wajam.nrv.protocol.Protocol

@RunWith(classOf[JUnitRunner])
class TestNettyTransport extends FunSuite with BeforeAndAfter {

  val host = InetAddress.getByName("0.0.0.0")
  val port = 54321
  val notifier = new Object()

  var nettyTransport: NettyTransport = null
  var mockProtocol: MockProtocol = null

  object TestEncoderDecoderFactory extends NettyTransportCodecFactory {

    def configureRequestEncoders(pipeline: ChannelPipeline) {
      pipeline.addLast("encoder", encoder)
    }

    def configureResponseEncoders(pipeline: ChannelPipeline) {
      pipeline.addLast("encoder", encoder)
    }

    def configureRequestDecoders(pipeline: ChannelPipeline) {
      pipeline.addLast("decoder", decoder)
    }

    def configureResponseDecoders(pipeline: ChannelPipeline) {
      pipeline.addLast("decoder", decoder)
    }

    val encoder = new StringEncoder() {
      override def encode(ctx: ChannelHandlerContext, channel: Channel, msg: AnyRef) = {
        super.encode(ctx, channel, msg.toString)
      }
    }

    val decoder = new StringDecoder() {
      override def decode(ctx: ChannelHandlerContext, channel: Channel, msg: AnyRef) = {
        val message = new InMessage()
        message.parameters += ("text" -> super.decode(ctx, channel, msg))
        message
      }
    }
  }

  class MockProtocol extends Protocol("test") {

    override val transport = null
    var receivedMessage: String = null

    override def parse(message: AnyRef): Message = {
      receivedMessage = message.asInstanceOf[Message].parameters.getOrElse("text", "").asInstanceOf[String]
      notifier.synchronized {
        notifier.notify()
      }
      null
    }

    override def start() {}

    override def stop() {}

    /**
     * Generate a transport message from a standard Message object.
     *
     * @param message The standard Message object
     * @return The message to be sent of the network
     */
    def generate(message: Message) = null
  }

  before {
    mockProtocol = new MockProtocol
    nettyTransport = new NettyTransport(host, port, mockProtocol) {
      val factory = TestEncoderDecoderFactory
    }
    nettyTransport.start()
  }

  after {
    nettyTransport.stop()
  }

  test("send message to self") {
    nettyTransport.sendMessage(new InetSocketAddress("127.0.0.1", port), "hello", true)

    notifier.synchronized {
      notifier.wait(1000)
    }

    assert(mockProtocol.receivedMessage != null)
    assert(mockProtocol.receivedMessage.equals("hello"))
  }

  test("send message to invalid destination") {
    var result: AnyRef = null
    nettyTransport.sendMessage(new InetSocketAddress("127.0.0.1:", 12344), "hello", true,
      (opResult: Option[Throwable]) => {
        opResult match {
          case None => fail()
          case Some(t) => result = t
        }
      })

    assert(result.isInstanceOf[Throwable])
  }


}
