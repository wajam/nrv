package com.wajam.nrv.transport.netty

import java.util.concurrent.Executors
import org.jboss.netty.channel.group.DefaultChannelGroup
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory
import org.jboss.netty.bootstrap.{ClientBootstrap, ServerBootstrap}
import com.wajam.nrv.protocol.Protocol
import org.jboss.netty.channel._
import com.wajam.nrv.Logging
import java.net.{InetAddress, InetSocketAddress}
import com.wajam.nrv.transport.Transport

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
  val connectionPool = new NettyConnectionPool(10000, 100)
  val client = new NettyClient(factory)
  val allChannels = new DefaultChannelGroup

  override def start() {
    server.start()
    client.start()
  }

  override def stop() {
    allChannels.close().awaitUninterruptibly()
    allChannels.clear()
    server.stop()
    client.stop()
  }

  override def sendMessage(destination: InetSocketAddress, message: AnyRef, completionCallback: Option[Throwable] => Unit = (_) => {}) {
    var writeChannel: Channel = null
    val pooledConnection = connectionPool.getPooledConnection(destination)
    pooledConnection match {
      case Some(channel) => writeChannel = channel
      case None => {
        writeChannel = client.openConnection(destination)
      }
    }
    val future = writeChannel.write(message)
    future.addListener(new ChannelFutureListener {
      override def operationComplete(p1: ChannelFuture) {
        val t = p1.getCause()
        if (t == null) {
          completionCallback(None)
          connectionPool.poolConnection(destination, writeChannel)
        } else {
          completionCallback(Some(t))
          writeChannel.close()
        }
      }
    })
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
        newPipeline.addLast("decoder", factory.createRequestDecoder())
        newPipeline.addLast("encoder", factory.createResponseEncoder())
        newPipeline.addLast("handler", incomingMessageHandler)
        newPipeline
      }
    }

  }

  class NettyClient(factory: NettyTransportCodecFactory) extends Logging {

    val clientBootstrap = new ClientBootstrap(new NioClientSocketChannelFactory(Executors.newCachedThreadPool, Executors.newCachedThreadPool))

    clientBootstrap.setPipelineFactory(new DefaultPipelineFactory)

    def start() {
      log.info("Starting client")
    }

    def stop() {
      clientBootstrap.releaseExternalResources()
      log.info("Stopping client")
    }

    def openConnection(destination: InetSocketAddress): Channel = {
      //todo pool connections
      log.info("Opening connection to: {}", destination)
      val connectFuture = clientBootstrap.connect(destination)
      val channel = connectFuture.awaitUninterruptibly.getChannel
      allChannels.add(channel)
      channel
    }

    class DefaultPipelineFactory extends ChannelPipelineFactory {
      override def getPipeline = {
        val newPipeline = Channels.pipeline()
        newPipeline.addLast("decoder", factory.createResponseDecoder())
        newPipeline.addLast("encoder", factory.createRequestEncoder())
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
      val message = protocol.parse(e.getMessage)
      protocol.handleIncoming(null, message, Unit=>{})
    }

    override def channelOpen(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
      allChannels.add(ctx.getChannel)
    }

    override def channelClosed(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
      allChannels.remove(ctx.getChannel)
    }
  }

}
