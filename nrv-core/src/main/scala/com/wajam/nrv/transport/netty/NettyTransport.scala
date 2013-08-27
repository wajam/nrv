package com.wajam.nrv.transport.netty

import java.util.concurrent.Executors
import org.jboss.netty.channel.group.DefaultChannelGroup
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory
import org.jboss.netty.bootstrap.{ClientBootstrap, ServerBootstrap}
import com.wajam.nrv.protocol.{HttpProtocol, Protocol}
import org.jboss.netty.channel._
import com.wajam.nrv.Logging
import java.net.{InetAddress, InetSocketAddress}
import org.jboss.netty.util.HashedWheelTimer
import com.wajam.nrv.transport.Transport
import org.jboss.netty.handler.timeout._
import java.nio.channels.ClosedChannelException
import java.util.concurrent.atomic.{AtomicReference, AtomicBoolean}

/**
 * Transport implementation based on Netty.
 *
 * User: felix
 * Date: 04/04/12
 */

abstract class NettyTransport(host: InetAddress,
                              port: Int,
                              protocol: Protocol,
                              idleTimeoutMs: Long,
                              maxPoolSize: Int) extends Transport(host, port, protocol) {

  val factory: NettyTransportCodecFactory

  val idleTimeoutIsSec = 30
  val connectionTimeoutInMs = 5000
  val connectionPool = new NettyConnectionPool(idleTimeoutMs, maxPoolSize)
  val allChannels = new AtomicReference[DefaultChannelGroup](new DefaultChannelGroup)
  val timer = new HashedWheelTimer()

  val started = new AtomicBoolean(false)

  lazy val server = new NettyServer(host, port, factory)
  lazy val client = new NettyClient(factory)

  override def start() {
    if (started.compareAndSet(false, true)) {
      allChannels.set(new DefaultChannelGroup)
      server.start()
      client.start()
    }
  }

  override def stop() {
    if (started.compareAndSet(true, false)) {
      //save current channel group reference
      val channelsToClose = allChannels.get()

      //switch to ShutdownChannelGroup, so new established connections will be closed
      allChannels.set(ShutdownChannelGroup)

      //close all channels
      channelsToClose.close().awaitUninterruptibly()
      channelsToClose.clear()

      server.stop()
      client.stop()
      timer.stop()
    }
  }

  def sendResponse(connection: AnyRef,
                   message: AnyRef,
                   closeAfter: Boolean,
                   completionCallback: Option[Throwable] => Unit = (_) => {}) {
    val channel = connection.asInstanceOf[Channel]
    //write the response on the channel but do not add the connection to the pool
    writeOnChannel(channel, message, None, completionCallback, closeAfter, canBePooled = false)
  }

  override def sendMessage(destination: InetSocketAddress,
                           message: AnyRef,
                           closeAfter: Boolean,
                           completionCallback: Option[Throwable] => Unit = (_) => {}) {

    val optConnection = connectionPool.getPooledConnection(destination) match {
      case pooledConnection @ Some(_) => pooledConnection
      case None => {
        try {
          Some(client.openConnection(destination))
        } catch {
          case e: Exception => {
            completionCallback(Some(e))
            None
          }
        }
      }
    }

    optConnection.foreach { connection =>
      //write the message on the connection and pool only if not closeAfter
      writeOnChannel(connection, message, Some(destination), completionCallback, closeAfter, !closeAfter)
    }
  }

  private def writeOnChannel(channel: Channel,
                             message: AnyRef,
                             destination: Option[InetSocketAddress],
                             completionCallback: Option[Throwable] => Unit = (_) => {},
                             closeAfter: Boolean,
                             canBePooled: Boolean) {
    val future = message match {
      case chunkedMessage:HttpProtocol.HttpChunkedMessage => {
        channel.write(chunkedMessage.begin)
        chunkedMessage.chunks.foreach(channel.write(_))
        channel.write(chunkedMessage.emptyChunk)
        channel.write(chunkedMessage.trailer)
      }
      case a => channel.write(a)
    }
    future.addListener(new ChannelFutureListener {
      override def operationComplete(p1: ChannelFuture) {
        val t = p1.getCause
        if (t == null) {
          completionCallback(None)
          destination match {
            case Some(dest) => {
              if (canBePooled) {
                val added = connectionPool.poolConnection(dest, channel)
                if(!added) {
                  //connection could not be added to the pool, close it
                  channel.close()
                }
              }
            }
            case None =>
          }
        } else {
          completionCallback(Some(t))
          channel.close()
        }
      }
    })
    if(closeAfter) {
      future.addListener(ChannelFutureListener.CLOSE)
    } else {
      future.addListener(ChannelFutureListener.CLOSE_ON_FAILURE)
    }
  }

  class NettyServer(host: InetAddress, port: Int, factory: NettyTransportCodecFactory) extends Logging {

    val serverMessageHandler = new MessageHandler(true)

    val serverBootstrap = new ServerBootstrap(
      new org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory(
        Executors.newCachedThreadPool(),
        Executors.newCachedThreadPool()))

    serverBootstrap.setPipelineFactory(new DefaultPipelineFactory)

    def start() {
      log.info("Starting server. host={} port={}", host, port)
      allChannels.get.add(serverBootstrap.bind(new java.net.InetSocketAddress(host, port)))
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
        factory.configureRequestDecoders(newPipeline)
        factory.configureResponseEncoders(newPipeline)
        newPipeline.addLast("idle", new IdleStateHandler(timer, 0, 0, idleTimeoutIsSec))
        newPipeline.addLast("handler", serverMessageHandler)
        newPipeline
      }
    }

  }

  class NettyClient(factory: NettyTransportCodecFactory) extends Logging {

    val clientMessageHandler = new MessageHandler(false)

    val clientBootstrap = new ClientBootstrap(new NioClientSocketChannelFactory(Executors.newCachedThreadPool, Executors.newCachedThreadPool))
    clientBootstrap.setOption("connectTimeoutMillis", connectionTimeoutInMs)
    clientBootstrap.setPipelineFactory(new DefaultPipelineFactory)

    def start() {
      log.info("Starting client")
    }

    def stop() {
      clientBootstrap.releaseExternalResources()
      log.info("Stopping client")
    }

    def openConnection(destination: InetSocketAddress): Channel = {
      if (started.get()) {
        log.debug("Opening connection to: {}", destination)
        val connectFuture = clientBootstrap.connect(destination)
        val channel = connectFuture.awaitUninterruptibly.getChannel
        allChannels.get.add(channel)
        channel
      } else {
        throw new RuntimeException("Transport closed: " + protocol.name + "[" + host + ":" + port + "]")
      }
    }

    class DefaultPipelineFactory extends ChannelPipelineFactory {
      override def getPipeline = {
        val newPipeline = Channels.pipeline()
        factory.configureResponseDecoders(newPipeline)
        factory.configureRequestEncoders(newPipeline)
        newPipeline.addLast("idle", new IdleStateHandler(timer, 0, 0, idleTimeoutIsSec))
        newPipeline.addLast("handler", clientMessageHandler)
        newPipeline
      }
    }
  }

  class MessageHandler(isServer: Boolean) extends IdleStateAwareChannelHandler with Logging {

    override def channelIdle(ctx: ChannelHandlerContext , e: IdleStateEvent) {
      log.trace("Connection was idle for {} sec and will be closed.", idleTimeoutIsSec)
      e.getChannel.close()
    }

    override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) {
      e.getCause match {
        case closeException: ClosedChannelException => {
          log.debug("Close channel exception caught: {}", closeException.toString)
        }
        case ex => {
          log.debug("Closing connection because of an exception: ", ex)
          e.getChannel.close()
        }
      }
    }

    override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
      log.debug("Received a message on connection {}: {}", ctx.getChannel, e.getMessage)
      messageReceivedEvent()
      protocol.transportMessageReceived(e.getMessage, Some(ctx.getChannel), Map())
    }

    override def channelOpen(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
      log.trace("New connection opened: {}", ctx.getChannel)
      connectionEstablishedEvent(isServer)
      allChannels.get.add(ctx.getChannel)
    }

    override def channelClosed(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
      log.trace("Connection closed: {}", ctx.getChannel)
      connectionClosedEvent(isServer)
      allChannels.get.remove(ctx.getChannel)
    }

    override def writeRequested(ctx: ChannelHandlerContext, e: MessageEvent) {
      log.debug("Sending a message on connection {}: {}", ctx.getChannel, e.getMessage)
      super.writeRequested(ctx, e)
    }

    override def writeComplete(ctx: ChannelHandlerContext, e: WriteCompletionEvent) {
      log.trace("Message sent on connection {}", ctx.getChannel)
      messageSentEvent()
      super.writeComplete(ctx, e)
    }
  }

  /**
   * Channel group that closes connections as they are added to the group.
   *
   * It is used in the shutdown process to avoid creating new connections while releasing the Netty resources.
   */
  object ShutdownChannelGroup extends DefaultChannelGroup {
    override def add(channel: Channel) = {
      channel.close()
      false
    }
  }
}
