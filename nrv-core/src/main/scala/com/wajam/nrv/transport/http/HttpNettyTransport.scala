package com.wajam.nrv.transport.http

import java.net.InetAddress
import com.wajam.nrv.protocol.Protocol
import com.wajam.nrv.transport.netty.{NettyTransportCodecFactory, NettyTransport}
import org.jboss.netty.channel.ChannelPipeline
import org.jboss.netty.handler.codec.http._

/**
 * This class...
 *
 * User: felix
 * Date: 09/04/12
 */

class HttpNettyTransport(host: InetAddress, port: Int, protocol: Protocol)
  extends NettyTransport(host, port, protocol) {

  val factory = HttpNettyTransportCodecFactory
}

object HttpNettyTransportCodecFactory extends NettyTransportCodecFactory {

  def configureRequestEncoders(pipeline: ChannelPipeline) {
    pipeline.addLast("encoder", new HttpRequestEncoder())
  }

  def configureResponseEncoders(pipeline: ChannelPipeline) {
    pipeline.addLast("encoder",  new HttpResponseEncoder())
    pipeline.addLast("deflater", new HttpContentCompressor())
  }

  def configureRequestDecoders(pipeline: ChannelPipeline) {
    pipeline.addLast("decoder", new HttpRequestDecoder())
  }

  def configureResponseDecoders(pipeline: ChannelPipeline) {
    pipeline.addLast("decoder",  new HttpResponseDecoder())
  }
}