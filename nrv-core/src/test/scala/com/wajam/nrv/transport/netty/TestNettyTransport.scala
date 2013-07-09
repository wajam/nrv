package com.wajam.nrv.transport.netty

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.jboss.netty.handler.codec.string.{StringEncoder, StringDecoder}
import org.jboss.netty.channel._
import java.net.{InetSocketAddress, InetAddress}
import com.wajam.nrv.data.{MString, InMessage, Message}
import com.wajam.nrv.protocol.Protocol
import com.wajam.nrv.cluster.Node

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
        message.parameters += ("text" -> super.decode(ctx, channel, msg).toString)
        message
      }
    }
  }

  class MockProtocol extends Protocol("test", null) {
    var receivedURI: String = null

    var receivedMessage: String = null

    override def parse(message: AnyRef, connection: AnyRef): Message = {
      val mockMessage = message.asInstanceOf[Message]

      receivedMessage = mockMessage.parameters.getOrElse("text", "").asInstanceOf[MString].value
      notifier.synchronized {
        notifier.notify()
      }
      mockMessage
    }

    override def start() {}

    override def stop() {}

    def generateMessage(message: Message, destination: Node): AnyRef = null

    def generateResponse(message: Message, connection: AnyRef): AnyRef = null

    def sendMessage(destination: Node, message: AnyRef, closeAfter: Boolean, completionCallback: (Option[Throwable]) => Unit) {}

    def sendResponse(connection: AnyRef, message: AnyRef, closeAfter: Boolean, completionCallback: (Option[Throwable]) => Unit) {}
  }


  before {
    mockProtocol = new MockProtocol
    nettyTransport = new NettyTransport(host, port, mockProtocol, 10000, 100) {
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
