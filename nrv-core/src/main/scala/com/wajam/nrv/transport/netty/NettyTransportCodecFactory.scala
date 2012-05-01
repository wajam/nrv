package com.wajam.nrv.transport.netty

import org.jboss.netty.channel.{ChannelUpstreamHandler, ChannelDownstreamHandler}


/**
 * Transport codec factory for netty transport.
 */

trait NettyTransportCodecFactory {

  def createRequestEncoder(): ChannelDownstreamHandler

  def createResponseEncoder(): ChannelDownstreamHandler

  def createRequestDecoder(): ChannelUpstreamHandler

  def createResponseDecoder(): ChannelUpstreamHandler

}
