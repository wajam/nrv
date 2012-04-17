package com.wajam.nrv.transport.nrv

import com.wajam.nrv.transport.netty.NettyTransportCodecFactory
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder
import org.jboss.netty.channel.{Channel, ChannelHandlerContext}
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import org.jboss.netty.handler.codec.frame.{CorruptedFrameException, FrameDecoder}
import com.wajam.nrv.codec.NrvCodec
import com.wajam.nrv.data.Message

/**
 * Codec factory for NRV protocol. This class provides the channel handlers used by netty
 * to convert bytes received from the network into structured objects, Message in this case.
 */

class NrvNettyTransportCodecFactory extends NettyTransportCodecFactory {

  val codec = new NrvCodec()

  def createRequestEncoder() = new NrvEncoder(codec)

  def createResponseEncoder() = new NrvEncoder(codec)

  def createRequestDecoder() = new NrvDecoder(codec)

  def createResponseDecoder() = new NrvDecoder(codec)
}

class NrvEncoder(codec: NrvCodec) extends OneToOneEncoder {
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

class NrvDecoder(codec: NrvCodec) extends FrameDecoder {
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
    codec.decode(bytes)
  }
}