package com.wajam.nrv.transport.netty

import org.jboss.netty.channel.{ChannelUpstreamHandler, ChannelDownstreamHandler}


/**
 * This class...
 *
 * User: felix
 * Date: 04/04/12
 */

trait NettyTransportCodecFactory {

  def createRequestEncoder(): ChannelDownstreamHandler

  def createResponseEncoder(): ChannelDownstreamHandler

  def createRequestDecoder(): ChannelUpstreamHandler

  def createResponseDecoder(): ChannelUpstreamHandler

}
