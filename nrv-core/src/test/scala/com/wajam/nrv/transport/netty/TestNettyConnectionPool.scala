package com.wajam.nrv.transport.netty

import org.scalatest.{BeforeAndAfter, FunSuite}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.jboss.netty.channel.Channel
import org.scalatest.Matchers._
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._
import java.net.{InetSocketAddress, SocketAddress}

@RunWith(classOf[JUnitRunner])
class TestNettyConnectionPool extends FunSuite with BeforeAndAfter with MockitoSugar {

  val destination = new InetSocketAddress("127.0.0.1", 8008)
  val timeout = 5000
  var pool: NettyConnectionPool = _
  var currentTime = 0L

  before {
    pool = new NettyConnectionPool(timeout, 100) {
      override protected def getTime() = currentTime
    }
  }

  test("should pool connection") {
    pool.poolConnection(destination, DummyChannel)
    val channel = pool.getPooledConnection(destination).get

    channel should equal (DummyChannel)
  }

  test("should reject if max size is reached") {
    pool = new NettyConnectionPool(timeout, 1)

    pool.poolConnection(destination, DummyChannel)
    pool.poolConnection(destination, DummyChannel) should be (false)

  }

  test("should allow if size less than maximum") {
    pool = new NettyConnectionPool(timeout, 1)

    pool.poolConnection(destination, DummyChannel)
    pool.poolConnection(destination, DummyChannel) should be (false)

    pool.getPooledConnection(destination)

    pool.poolConnection(destination, DummyChannel) should be (true)
  }

  test("should return None if empty") {
    pool.getPooledConnection(destination) should equal (None)
  }

  test("should expire connection after timeout") {
    pool.poolConnection(destination, DummyChannel)
    currentTime += timeout
    pool.getPooledConnection(destination) should equal (None)
  }

  test("should free a space in the pool once a pool connection expires") {
    val oneConnectionPool = new NettyConnectionPool(timeout, 1) {
      override protected def getTime() = currentTime
    }
    oneConnectionPool.poolConnection(destination, DummyChannel)
    currentTime += timeout
    oneConnectionPool.getPooledConnection(destination) should equal (None)

    oneConnectionPool.poolConnection(destination, DummyChannel)
    oneConnectionPool.getPooledConnection(destination) should equal (Some(DummyChannel))
  }

  test("should close channel on expiration") {
    val mockChannel = mock[Channel]
    when(mockChannel.isOpen).thenReturn(true)
    pool.poolConnection(destination, mockChannel)
    currentTime += timeout
    pool.getPooledConnection(destination)

    verify(mockChannel).close()
  }

  test("should not pool closed connection") {
    val mockChannel = mock[Channel]
    when(mockChannel.isOpen).thenReturn(false)
    pool.poolConnection(destination, mockChannel) should be (false)

    pool.getPooledConnection(destination) should equal (None)
  }

  test("should not reuse closed connection") {
    val mockChannel = mock[Channel]
    when(mockChannel.isOpen).thenReturn(true)
    pool.poolConnection(destination, mockChannel) should be (true)

    when(mockChannel.isOpen).thenReturn(false)
    pool.getPooledConnection(destination) should equal (None)
  }

  object DummyChannel extends Channel {
    def getId = null

    def getAttachment = null

    def setAttachment(attachment: Any) {}

    def getFactory = null

    def getParent = null

    def getConfig = null

    def getPipeline = null

    def isOpen = true

    def isBound = false

    def isConnected = false

    def getLocalAddress = null

    def getRemoteAddress = null

    def write(message: AnyRef) = null

    def write(message: AnyRef, remoteAddress: SocketAddress) = null

    def bind(localAddress: SocketAddress) = null

    def connect(remoteAddress: SocketAddress) = null

    def disconnect() = null

    def unbind() = null

    def close() = null

    def getCloseFuture = null

    def getInterestOps = 0

    def isReadable = false

    def isWritable = false

    def setInterestOps(interestOps: Int) = null

    def setReadable(readable: Boolean) = null

    def compareTo(o: Channel) = 0
  }
}
