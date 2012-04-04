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

  def start() {
    server.start()
    client.start()
  }

  def stop() {
    server.stop()
    client.stop()
  }

  def sendMessage(host: InetAddress, port: Int, message: Message) {
    client.openConnection(host, port).write(message).addListener(ChannelFutureListener.CLOSE)
  }

  class NettyServer(host: InetAddress, port: Int, factory: NettyTransportCodecFactory) extends Stoppable {

    val serverBootstrap = new ServerBootstrap(
      new org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory(
        Executors.newCachedThreadPool(),
        Executors.newCachedThreadPool()))

    serverBootstrap.setPipelineFactory(new DefaultPipelineFactory)

    override def start() {
      super.start()
      allChannels.add(serverBootstrap.bind(new java.net.InetSocketAddress(host, port)))
    }

    override def stop() {
      super.stop()
      serverBootstrap.releaseExternalResources()
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

  class NettyClient(factory: NettyTransportCodecFactory) extends Stoppable {

    val clientBootstrap = new ClientBootstrap(new NioClientSocketChannelFactory(Executors.newCachedThreadPool, Executors.newCachedThreadPool))

    val clientHandler = new SimpleChannelUpstreamHandler {
      override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
        protocol.handleIncoming(null, e.getMessage.asInstanceOf[Message])
      }
    }

    clientBootstrap.setPipelineFactory(new DefaultPipelineFactory)

    override def start() {
      super.start()
    }

    override def stop() {
      super.stop()
      clientBootstrap.releaseExternalResources()
    }

    def openConnection(host: InetAddress, port: Int): Channel = {
      //todo pool connections
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

  val incomingMessageHandler = new SimpleChannelUpstreamHandler {
    override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
      protocol.handleIncoming(null, e.getMessage.asInstanceOf[Message])
    }
  }

  trait Stoppable {
    val allChannels = new DefaultChannelGroup

    def start() {
      allChannels.clear()
    }

    def stop() {
      allChannels.close().awaitUninterruptibly()
    }
  }

}
