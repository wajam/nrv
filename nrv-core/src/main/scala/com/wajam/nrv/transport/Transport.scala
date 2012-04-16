package com.wajam.nrv.transport

import com.wajam.nrv.protocol.Protocol
import java.net.{InetSocketAddress, URI, InetAddress}
import com.wajam.nrv.data.Message

/**
 * This class...
 *
 * User: felix
 * Date: 04/04/12
 */

abstract class Transport (host: InetAddress, port: Int, protocol: Protocol) {

  def start()

  def stop()

  def sendMessage(destination: InetSocketAddress, message: AnyRef,
                  completionCallback: Option[Throwable] => Unit = (_) => {})

}

class TransportMessage(var sendResponseCallback: (Message) => Unit) extends Message

