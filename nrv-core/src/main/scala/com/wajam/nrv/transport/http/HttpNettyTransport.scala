package com.wajam.nrv.transport.http

import java.net.InetAddress
import org.jboss.netty.handler.codec.oneone.{OneToOneDecoder, OneToOneEncoder}
import org.jboss.netty.handler.stream.ChunkedWriteHandler
import org.jboss.netty.handler.codec.http._
import org.jboss.netty.channel.{ChannelFuture, Channel, ChannelHandlerContext, ChannelPipeline}
import com.yammer.metrics.scala.Instrumented
import com.wajam.nrv.protocol.Protocol
import com.wajam.nrv.transport.netty.{NettyTransportCodecFactory, NettyTransport}
import com.wajam.nrv.protocol.HttpProtocol.{HttpChunkedResponse, CHUNK_EMPTY, CHUNK_TRAILER}

/**
 * HTTP transport implementation backed by netty.
 */

class HttpNettyTransport(host: InetAddress,
                         port: Int, protocol: Protocol,
                         idleConnectionTimeoutMs: Long,
                         maxConnectionPoolSize: Int)
  extends NettyTransport(host, port, protocol, idleConnectionTimeoutMs, maxConnectionPoolSize) {

  val MAX_SIZE = 1048576 //1M
  val factory = new HttpNettyTransportCodecFactory

  override def writeOnChannel(channel: Channel, message: AnyRef): ChannelFuture = {
    message match {
      case httpMessage: HttpMessage =>
        channel.write(httpMessage)

      case chunkedResponse: HttpChunkedResponse =>
        channel.write(chunkedResponse.begin)
        channel.write(chunkedResponse.input)
        channel.write(CHUNK_EMPTY)
        channel.write(CHUNK_TRAILER)
    }
  }

  class HttpNettyTransportCodecFactory extends NettyTransportCodecFactory {

    def configureRequestEncoders(pipeline: ChannelPipeline) {
      pipeline.addLast("encoder", new HttpRequestEncoder())
    }

    def configureResponseEncoders(pipeline: ChannelPipeline) {
      pipeline.addLast("encoder", new HttpResponseEncoder())
      pipeline.addLast("deflater", new HttpContentCompressor())
      pipeline.addLast("chunkedWriter", new ChunkedWriteHandler())
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

    val success = metrics.meter(prefix+"-http-2xx", "success")
    val clientFailure = metrics.meter(prefix+"-http-4xx", "protocol-error")
    val notFount = metrics.meter(prefix+"-http-404", "not-found")
    val serverFailure = metrics.meter(prefix+"-http-5xx", "server-error")
    val other = metrics.meter(prefix+"-http-other", "other-error")

    def updateCounter(response: HttpResponse) {
      response.getStatus.getCode / 100 match {
        case 2 => success.mark()
        case 4 => {
          if (response.getStatus.getCode == 404) {
            notFount.mark()
          }
          clientFailure.mark()
        }
        case 5 => serverFailure.mark()
        case _ => other.mark()
      }
    }
  }
}