package com.wajam.nrv.transport.netty

import java.util.concurrent.Executors
import org.jboss.netty.channel.group.DefaultChannelGroup
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory
import org.jboss.netty.bootstrap.{ClientBootstrap, ServerBootstrap}
import java.net.{InetAddress, InetSocketAddress}
import com.wajam.nrv.transport.{Transport}
import com.wajam.nrv.protocol.Protocol
import org.jboss.netty.channel._
import com.wajam.nrv.data.Message
import com.wajam.nrv.Logging

/**
 * Transport implementation based on Netty.
 *
 * User: felix
 * Date: 04/04/12
 */

class NettyTransport(host: InetAddress,
                     port: Int,
                     protocol: Protocol,
                     factory: NettyTransportCodecFactory) extends Transport(host, port, protocol) {

  val server = new NettyServer(host, port, factory)
  val client = new NettyClient(factory)
  val allChannels = new DefaultChannelGroup

  def start() {
    server.start()
    client.start()
  }

  def stop() {
    allChannels.close().awaitUninterruptibly()
    allChannels.clear()
    server.stop()
    client.stop()
  }

  def sendMessage(host: InetAddress, port: Int, message: Message) {
    client.openConnection(host, port).write(message).addListener(ChannelFutureListener.CLOSE)
  }

  class NettyServer(host: InetAddress, port: Int, factory: NettyTransportCodecFactory) extends Logging {

    val serverBootstrap = new ServerBootstrap(
      new org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory(
        Executors.newCachedThreadPool(),
        Executors.newCachedThreadPool()))

    serverBootstrap.setPipelineFactory(new DefaultPipelineFactory)

    def start() {
      log.info("Starting server. host={} port={}", host, port)
      allChannels.add(serverBootstrap.bind(new java.net.InetSocketAddress(host, port)))
      log.info("Server started.")
    }

    def stop() {
      log.info("Stopping server. host={} port={}", host, port)
      serverBootstrap.releaseExternalResources()
      log.info("Server stopped.")
    }

    class DefaultPipelineFactory extends ChannelPipelineFactory {
      override def getPipeline = {
        val newPipeline = Channels.pipeline()
        newPipeline.addLast("decoder", factory.createDecoder())
        newPipeline.addLast("encoder", factory.createEncoder())
        newPipeline.addLast("handler", incomingMessageHandler)
        newPipeline
      }
    }
  }

  class NettyClient(factory: NettyTransportCodecFactory) extends Logging {

    val clientBootstrap = new ClientBootstrap(new NioClientSocketChannelFactory(Executors.newCachedThreadPool, Executors.newCachedThreadPool))

    val clientHandler = new SimpleChannelUpstreamHandler {
      override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
        protocol.handleIncoming(null, e.getMessage.asInstanceOf[Message])
      }
    }

    clientBootstrap.setPipelineFactory(new DefaultPipelineFactory)

    def start() {
      log.info("Starting client")
    }

    def stop() {
      clientBootstrap.releaseExternalResources()
      log.info("Stopping client")
    }

    def openConnection(host: InetAddress, port: Int): Channel = {
      //todo pool connections
      log.info("Opening connection to {}:{}", host, port)
      val connectFuture = clientBootstrap.connect(new InetSocketAddress(host, port))
      val channel = connectFuture.awaitUninterruptibly.getChannel
      allChannels.add(channel)
      channel
    }

    class DefaultPipelineFactory extends ChannelPipelineFactory {
      override def getPipeline = {
        val newPipeline = Channels.pipeline()
        newPipeline.addLast("decoder", factory.createDecoder())
        newPipeline.addLast("encoder", factory.createEncoder())
        newPipeline.addLast("handler", incomingMessageHandler)
        newPipeline
      }
    }
  }

  val incomingMessageHandler = new SimpleChannelUpstreamHandler with Logging {

    override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) {
      e.getChannel.close()
      log.info("Connection closed because of an exception: ", e.getCause)
    }

    override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
      protocol.handleIncoming(null, e.getMessage.asInstanceOf[Message])
    }

    override def channelOpen(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
      allChannels.add(ctx.getChannel)
    }

    override def channelClosed(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
      allChannels.remove(ctx.getChannel)
    }
  }

}
