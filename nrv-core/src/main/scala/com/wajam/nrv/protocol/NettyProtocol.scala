package com.wajam.nrv.protocol

import com.wajam.nrv.codec.Codec
import com.wajam.nrv.cluster.Cluster
import java.util.concurrent.Executors
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder
import org.jboss.netty.channel._
import com.wajam.nrv.service.Action
import org.jboss.netty.bootstrap.{ClientBootstrap, ServerBootstrap}
import socket.nio.{NioClientSocketChannelFactory, NioServerSocketChannelFactory}
import org.jboss.netty.buffer.{ChannelBuffers, ChannelBuffer}
import org.jboss.netty.handler.codec.frame.{CorruptedFrameException, FrameDecoder}
import com.wajam.nrv.data.{InRequest, Message}
import java.net.{InetAddress, InetSocketAddress}

/**
 * Protocol that uses Netty (NIO sockets)
 */
abstract class NettyProtocol(name: String, cluster: Cluster, codec: Codec) extends Protocol(name, cluster, codec) {

  val srvBootstrap = new ServerBootstrap(new NioServerSocketChannelFactory(Executors.newCachedThreadPool, Executors.newCachedThreadPool))
  srvBootstrap.setPipelineFactory(new ChannelPipelineFactory {
    override def getPipeline = Channels.pipeline(
      new Encoder,
      new Decoder,
      new ServerHandler)
  })
  var srvChannel: Channel = null

  class ServerHandler extends SimpleChannelUpstreamHandler {
    override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
      handleIncoming(null, e.getMessage.asInstanceOf[Message])
    }
  }

  val cltBootstrap = new ClientBootstrap(new NioClientSocketChannelFactory(Executors.newCachedThreadPool, Executors.newCachedThreadPool))
  cltBootstrap.setPipelineFactory(new ChannelPipelineFactory {
    override def getPipeline = Channels.pipeline(
      new Encoder,
      new Decoder,
      new ServerHandler)
  })


  def getClientChannel(host: InetAddress, port: Int): Channel = {
    val connectFuture = this.cltBootstrap.connect(new InetSocketAddress(host, port))
    connectFuture.awaitUninterruptibly.getChannel
  }

  def start() {
    this.srvChannel = this.srvBootstrap.bind(new InetSocketAddress(cluster.localNode.host, cluster.localNode.ports.get(name).get))
  }

  def stop() {
    this.srvChannel.close()
    // TODO: stop client pool
  }

  def handleOutgoing(action: Action, message: Message) {
    val node = message.destination(0).node

    val channel = this.getClientChannel(node.host, node.ports(name))
    val future = channel.write(message)
    future.awaitUninterruptibly

    // TODO: repool
    channel.close.awaitUninterruptibly
  }


  class Encoder extends OneToOneEncoder {
    override def encode(ctx: ChannelHandlerContext, channel: Channel, msg: AnyRef): Object = {
      msg match {
        case m: Message =>
        // Ignore what this encoder can't encode.
        case _ => return msg
      }

      // Convert to a message first for easier implementation.
      val v = msg match {
        case m: Message => m
        case _ => null
      }

      // Convert the number into a byte array
      val data = encodeMessage(v)
      val dataLength = data.length

      // Construct a message
      val buf = ChannelBuffers.dynamicBuffer
      buf.writeByte('F'.toByte)
      buf.writeInt(dataLength)
      buf.writeBytes(data)

      buf
    }
  }

  class Decoder extends FrameDecoder {
    override def decode(ctx: ChannelHandlerContext, channel: Channel, buffer: ChannelBuffer): Object = {
      // Wait until the length prefix is available.
      if (buffer.readableBytes < 5) {
        return null
      }

      buffer.markReaderIndex()

      // Check the magic number.
      val magicNumber: Int = buffer.readUnsignedByte
      if (magicNumber != 'F') {
        buffer.resetReaderIndex()
        throw new CorruptedFrameException("Invalid magic number: " + magicNumber)
      }

      // Wait until the whole data is available.
      val dataLength = buffer.readInt
      if (buffer.readableBytes < dataLength) {
        buffer.resetReaderIndex()
        return null
      }

      // Convert the received data into a new BigInteger.
      val bytes = new Array[Byte](dataLength)
      buffer.readBytes(bytes)

      // Return the real thing
      val inMsg = decodeMessage(bytes)
      val req = new InRequest()
      req.loadData(inMsg)

      req
    }
  }

}
