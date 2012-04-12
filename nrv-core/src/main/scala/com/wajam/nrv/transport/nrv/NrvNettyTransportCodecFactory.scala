package com.wajam.nrv.transport.nrv

import com.wajam.nrv.transport.netty.NettyTransportCodecFactory
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder
import org.jboss.netty.channel.{Channel, ChannelHandlerContext}
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import org.jboss.netty.handler.codec.frame.{CorruptedFrameException, FrameDecoder}
import com.wajam.nrv.codec.NrvCodec
import com.wajam.nrv.data.{OutRequest, InRequest, Message}

/**
 * This class...
 *
 * User: felix
 * Date: 09/04/12
 */

class NrvNettyTransportCodecFactory extends NettyTransportCodecFactory {

  val codec = new NrvCodec()


  def createRequestEncoder() = new Encoder(codec)

  def createResponseEncoder() = new Encoder(codec)

  def createRequestDecoder() = new Decoder(codec)

  def createResponseDecoder() = new Decoder(codec)
}

class Encoder(codec: NrvCodec) extends OneToOneEncoder {
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

class Decoder(codec: NrvCodec) extends FrameDecoder {
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