package com.wajam.nrv.transport

import java.net.InetAddress
import com.wajam.nrv.protocol.Protocol

/**
 * This class...
 *
 * User: felix
 * Date: 04/04/12
 */

abstract class Transport (host: InetAddress, port: Int, protocol: Protocol) {

  def start()

  def stop()

  def sendMessage(host: InetAddress, port: Int, message: AnyRef,
                  completionCallback: Option[Throwable] => Unit = (_) => {})
}
