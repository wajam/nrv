package com.wajam.nrv.transport.netty

import org.jboss.netty.channel.{ChannelUpstreamHandler, ChannelDownstreamHandler}


/**
 * This class...
 *
 * User: felix
 * Date: 04/04/12
 */

trait NettyTransportCodecFactory {

  def createEncoder(): ChannelDownstreamHandler

  def createDecoder(): ChannelUpstreamHandler

}
