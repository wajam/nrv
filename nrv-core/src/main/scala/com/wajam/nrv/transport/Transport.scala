package com.wajam.nrv.transport

import com.wajam.nrv.protocol.Protocol
import java.net.{InetSocketAddress, InetAddress}
import com.wajam.nrv.data.Message
import com.yammer.metrics.scala.Instrumented

/**
 * Transport layer used to send and received messages from the network.
 */

abstract class Transport (host: InetAddress, port: Int, protocol: Protocol) extends Instrumented {

  protected val totalConnectionCounter = metrics.counter("current-connection-count")
  protected val clientConnectionCounter = metrics.counter("client-connection-count")
  protected val serverConnectionCounter = metrics.counter("server-connection-count")
  protected val inMessagePerSecond = metrics.meter("in-message-rate", "messages")
  protected val outMessagePerSecond = metrics.meter("out-message-rate", "messages")

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

  protected def connectionEstablishedEvent(server: Boolean) {
    totalConnectionCounter += 1
    if (server) {
      serverConnectionCounter += 1
    } else {
      clientConnectionCounter += 1
    }
  }

  protected def connectionClosedEvent(server: Boolean) {
    totalConnectionCounter -= 1
    if (server) {
      serverConnectionCounter -= 1
    } else {
      clientConnectionCounter -= 1
    }
  }

  protected def messageReceivedEvent() {
    inMessagePerSecond.mark()
  }

  protected def messageSentEvent() {
    outMessagePerSecond.mark()
  }
}

