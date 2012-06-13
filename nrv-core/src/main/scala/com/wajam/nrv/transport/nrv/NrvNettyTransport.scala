package com.wajam.nrv.transport.nrv

import com.wajam.nrv.transport.netty.{NettyTransportCodecFactory, NettyTransport}
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import org.jboss.netty.handler.codec.frame.{CorruptedFrameException, FrameDecoder}
import java.net.InetAddress
import com.wajam.nrv.protocol.Protocol
import org.jboss.netty.channel.{ChannelPipeline, ChannelHandlerContext, Channel}

/**
 * Transport layer for NRV
 */

class NrvNettyTransport(host: InetAddress, port: Int, protocol: Protocol)
  extends NettyTransport(host, port, protocol) {

  val factory = NrvNettyTransportCodecFactory
}

object NrvNettyTransportCodecFactory extends NettyTransportCodecFactory {

  def configureRequestEncoders(pipeline: ChannelPipeline) {
    pipeline.addLast("encoder", new NrvEncoder)
  }

  def configureResponseEncoders(pipeline: ChannelPipeline) {
    pipeline.addLast("encoder", new NrvEncoder)
  }

  def configureRequestDecoders(pipeline: ChannelPipeline) {
    pipeline.addLast("decoder", new NrvDecoder)
  }

  def configureResponseDecoders(pipeline: ChannelPipeline) {
    pipeline.addLast("decoder", new NrvDecoder)
  }

  class NrvEncoder extends OneToOneEncoder {
    override def encode(ctx: ChannelHandlerContext, channel: Channel, msg: AnyRef): Object = {
      msg match {
        case m: Array[Byte] =>
        // Ignore what this encoder can't encode.
        case _ => return msg
      }

      // Convert to a message first for easier implementation.
      val data = msg.asInstanceOf[Array[Byte]]
      val dataLength = data.length

      // write to buffer
      val buf = ChannelBuffers.dynamicBuffer
      buf.writeByte('F'.toByte)
      buf.writeInt(dataLength)
      buf.writeBytes(data)

      buf
    }
  }

  class NrvDecoder extends FrameDecoder {
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
      bytes
    }
  }
}

