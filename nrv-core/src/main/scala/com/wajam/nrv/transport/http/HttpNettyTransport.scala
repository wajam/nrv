package com.wajam.nrv.transport.http

import java.net.InetAddress
import com.wajam.nrv.protocol.Protocol
import com.wajam.nrv.transport.netty.{NettyTransportCodecFactory, NettyTransport}
import org.jboss.netty.channel.{Channel, ChannelHandlerContext, ChannelPipeline}
import org.jboss.netty.handler.codec.http._
import com.yammer.metrics.scala.Instrumented
import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.handler.codec.http.HttpMessageDecoder.State
import org.jboss.netty.handler.codec.oneone.{OneToOneDecoder, OneToOneEncoder}

/**
 * This class...
 *
 * User: felix
 * Date: 09/04/12
 */

class HttpNettyTransport(host: InetAddress, port: Int, protocol: Protocol)
  extends NettyTransport(host, port, protocol) {

  val MAX_SIZE = 1048576 //1M
  val factory = new HttpNettyTransportCodecFactory

  class HttpNettyTransportCodecFactory extends NettyTransportCodecFactory {

    def configureRequestEncoders(pipeline: ChannelPipeline) {
      pipeline.addLast("encoder", new HttpRequestEncoder())
    }

    def configureResponseEncoders(pipeline: ChannelPipeline) {
      pipeline.addLast("encoder", new HttpResponseEncoder())
      pipeline.addLast("deflater", new HttpContentCompressor())
      pipeline.addLast("metrics", new ServerHttpResponseMetricUpdater())
    }

    def configureRequestDecoders(pipeline: ChannelPipeline) {
      pipeline.addLast("decoder", new HttpRequestDecoder())
      pipeline.addLast("request-aggregator", new HttpChunkAggregator(MAX_SIZE))
    }

    def configureResponseDecoders(pipeline: ChannelPipeline) {
      pipeline.addLast("decoder", new HttpResponseDecoder())
      pipeline.addLast("response-aggregator", new HttpChunkAggregator(MAX_SIZE))
      pipeline.addLast("metrics", new ClientHttpResponseMetricUpdater())
    }
  }

  class ServerHttpResponseMetricUpdater extends OneToOneEncoder{
    val updater = new HttpResponseMetricUpdater("server")

    override def encode(channelContext: ChannelHandlerContext, channel: Channel, msg: AnyRef) = {
      msg match {
        case response: HttpResponse => updater.updateCounter(response)
        case _ =>
      }
      msg
    }
  }

  class ClientHttpResponseMetricUpdater extends OneToOneDecoder{
    val updater = new HttpResponseMetricUpdater("client")

    def decode(channelContext: ChannelHandlerContext, channel: Channel, msg: AnyRef) = {
      msg match {
        case response: HttpResponse => updater.updateCounter(response)
        case _ =>
      }
      msg
    }
  }

  class HttpResponseMetricUpdater(prefix: String) extends Instrumented {

    val success = metrics.counter(prefix+"-http-2xx")
    val clientFailure = metrics.counter(prefix+"-http-4xx")
    val serverFailure = metrics.counter(prefix+"-http-5xx")
    val other = metrics.counter(prefix+"-http-other")

    def updateCounter(response: HttpResponse) {
      response.getStatus.getCode / 100 match {
        case 2 => success += 1
        case 4 => clientFailure += 1
        case 5 => serverFailure += 1
        case _ => other += 1
      }
    }
  }
}