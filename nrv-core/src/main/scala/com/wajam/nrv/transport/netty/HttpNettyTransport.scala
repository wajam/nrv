package com.wajam.nrv.transport.netty

import java.net.InetAddress
import com.wajam.nrv.protocol.Protocol
import org.jboss.netty.handler.codec.http.{HttpResponseEncoder, HttpResponseDecoder, HttpRequestDecoder, HttpRequestEncoder}

/**
 * This class...
 *
 * User: felix
 * Date: 09/04/12
 */

class HttpNettyTransport(host: InetAddress, port: Int, protocol: Protocol) extends NettyTransport(host, port, protocol, HttpNettyTransportCodec) {

}

object HttpNettyTransportCodec extends NettyTransportCodecFactory {

  def createRequestEncoder() = new HttpRequestEncoder()

  def createResponseEncoder() = new HttpResponseEncoder()

  def createRequestDecoder() = new HttpRequestDecoder()

  def createResponseDecoder() = new HttpResponseDecoder()
}
