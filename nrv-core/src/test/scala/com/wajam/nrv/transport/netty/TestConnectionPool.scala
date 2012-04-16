package com.wajam.nrv.transport.netty

import org.scalatest.{BeforeAndAfter, FunSuite}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import java.net.SocketAddress
import org.jboss.netty.channel.Channel
import org.scalatest.matchers.ShouldMatchers._
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._

/**
 * This class...
 *
 * User: felix
 * Date: 16/04/12
 */

@RunWith(classOf[JUnitRunner])
class TestConnectionPool extends FunSuite with BeforeAndAfter with MockitoSugar {

  val timeout = 5000
  var pool: NettyConnectionPool = _
  var currentTime = 0L

  before {
    pool = new NettyConnectionPool(timeout, 100) {
      override protected def getTime() = currentTime
    }
  }

  test("should pool connection") {
    val uri = "http://host:80"
    pool.poolConnection(uri, DummyChannel)
    val channel = pool.getPooledConnection(uri).get

    channel should equal (DummyChannel)
  }

  test("should reject if max size is reached") {
    pool = new NettyConnectionPool(timeout, 1)

    val uri = "http://host:80"
    pool.poolConnection(uri, DummyChannel)
    pool.poolConnection(uri, DummyChannel) should be (false)

  }

  test("should allow is size less") {
    pool = new NettyConnectionPool(timeout, 1)

    val uri = "http://host:80"
    pool.poolConnection(uri, DummyChannel)
    pool.poolConnection(uri, DummyChannel) should be (false)

    pool.getPooledConnection(uri)

    pool.poolConnection(uri, DummyChannel) should be (true)
  }

  test("should return None if empty") {
    pool.getPooledConnection("uri") should equal (None)
  }

  test("should expire connection after timeout") {
    val uri = "http://host:80"
    pool.poolConnection(uri, DummyChannel)
    currentTime += timeout
    pool.getPooledConnection(uri) should equal (None)
  }

  test("should close channel on expiration") {
    val uri = "http://host:80"
    val mockChannel = mock[Channel]
    pool.poolConnection(uri, mockChannel)
    currentTime += timeout
    pool.getPooledConnection(uri)

    verify(mockChannel).close()
  }



  object DummyChannel extends Channel {
    def getId = null

    def getFactory = null

    def getParent = null

    def getConfig = null

    def getPipeline = null

    def isOpen = false

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
