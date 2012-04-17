package com.wajam.nrv.transport

import com.wajam.nrv.protocol.Protocol
import java.net.{InetSocketAddress, InetAddress}
import com.wajam.nrv.data.Message

/**
 * Transport layer used to send and received message from the network.
 */

abstract class Transport (host: InetAddress, port: Int, protocol: Protocol) {

  def start()

  def stop()

  def sendMessage(destination: InetSocketAddress, message: AnyRef,
                  completionCallback: Option[Throwable] => Unit = (_) => {})

}

class TransportMessage(var sendResponseCallback: (Message) => Unit) extends Message

