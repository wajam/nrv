package com.wajam.nrv.protocol

import com.wajam.nrv.cluster.Cluster
import com.wajam.nrv.codec.NrvCodec
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder
import org.jboss.netty.channel.{Channel, ChannelHandlerContext}
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import org.jboss.netty.handler.codec.frame.{CorruptedFrameException, FrameDecoder}
import com.wajam.nrv.data.{InRequest, Message}
import com.wajam.nrv.service.Action
import com.wajam.nrv.transport.netty.{NettyTransportCodecFactory, NettyTransport}

/**
 * Default protocol used by NRV. All nodes must have this protocol, since it's
 * used for cluster management.
 */
class NrvProtocol(cluster: Cluster) extends Protocol("nrv", cluster, new NrvCodec()) {

  val transport = new NettyTransport(cluster.localNode.host,
    cluster.localNode.ports.get(name).get,
    this,
    TransportCodecFactory)

  def start() {
    transport.start()
  }

  def stop() {
    transport.stop()
  }

  def handleOutgoing(action: Action, message: Message) {
    val node = message.destination(0).node

    transport.sendMessage(node.host, node.ports(name), message)
  }

   object TransportCodecFactory extends NettyTransportCodecFactory {
    override def createEncoder() = new Encoder

    override def createDecoder() = new Decoder
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
