package com.wajam.nrv.transport.nrv

import codec.Codec
import com.wajam.nrv.transport.netty.{NettyTransportCodecFactory, NettyTransport}
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder
import org.jboss.netty.channel.{Channel, ChannelHandlerContext}
import com.wajam.nrv.data.Message
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import org.jboss.netty.handler.codec.frame.{CorruptedFrameException, FrameDecoder}
import java.net.InetAddress
import com.wajam.nrv.protocol.Protocol

/**
 * Transport layer for NRV
 */

class NrvNettyTransport(host: InetAddress, port: Int, protocol: Protocol, codec: Codec)
  extends NettyTransport(host, port, protocol) {

  val factory = new NrvNettyTransportCodecFactory(codec)
}

class NrvNettyTransportCodecFactory(codec: Codec) extends NettyTransportCodecFactory {

  def createRequestEncoder() = new NrvEncoder(codec)

  def createResponseEncoder() = new NrvEncoder(codec)

  def createRequestDecoder() = new NrvDecoder(codec)

  def createResponseDecoder() = new NrvDecoder(codec)

  class NrvEncoder(codec: Codec) extends OneToOneEncoder {
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
      val data = codec.encode(v)
      val dataLength = data.length

      // Construct a message
      val buf = ChannelBuffers.dynamicBuffer
      buf.writeByte('F'.toByte)
      buf.writeInt(dataLength)
      buf.writeBytes(data)

      buf
    }
  }

  class NrvDecoder(codec: Codec) extends FrameDecoder {
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

      // Read all the bytes.
      val bytes = new Array[Byte](dataLength)
      buffer.readBytes(bytes)

      // Return the real thing
      codec.decode(bytes)
    }
  }
}

