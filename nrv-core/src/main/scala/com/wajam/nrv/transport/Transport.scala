package com.wajam.nrv.transport

import com.wajam.nrv.protocol.Protocol
import java.net.{InetSocketAddress, InetAddress}
import com.wajam.nrv.data.Message

/**
 * Transport layer used to send and received messages from the network.
 */

abstract class Transport (host: InetAddress, port: Int, protocol: Protocol) {

  /**
   * Start the transport layer. After this call, messages can be sent and received.
   */
  def start()

  /**
   * Stop the transport layer. After this call, no more messages can be sent and none
   * will be received.
   */
  def stop()

  /**
   * Send a message on the transport layer.
   *
   * @param destination Destination's address
   * @param message The message to send
   * @param closeAfter Tells the transport layer to close or not the connection after the message has been sent
   * @param completionCallback Callback executed once the message has been sent or when a failure occured
   */
  def sendMessage(destination: InetSocketAddress,
                  message: AnyRef,
                  closeAfter:Boolean,
                  completionCallback: Option[Throwable] => Unit = (_) => {})

  /**
   * Send a message as a response on a specific connection.
   *
   * @param connection The connection on which to send the message
   * @param message The message to send
   * @param closeAfter Tells the transport layer to close or not the connection after the message has been sent
   * @param completionCallback Callback executed once the message has been sent or when a failure occured
   */
  def sendResponse(connection: AnyRef,
                   message: AnyRef,
                   closeAfter: Boolean,
                   completionCallback: Option[Throwable] => Unit = (_) => {})
}

