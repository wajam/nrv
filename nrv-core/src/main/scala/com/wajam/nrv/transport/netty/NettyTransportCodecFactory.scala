package com.wajam.nrv.transport.netty

import org.jboss.netty.channel.{ChannelPipeline, ChannelUpstreamHandler, ChannelDownstreamHandler}


/**
 * Transport codec factory for netty transport.
 */

trait NettyTransportCodecFactory {

  def configureRequestEncoders(pipeline: ChannelPipeline)

  def configureResponseEncoders(pipeline: ChannelPipeline)

  def configureRequestDecoders(pipeline: ChannelPipeline)

  def configureResponseDecoders(pipeline: ChannelPipeline)

}
