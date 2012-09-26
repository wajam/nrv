package com.wajam.nrv.protocol

import com.wajam.nrv.Logging
import com.wajam.nrv.service.{MessageHandler, Action}
import java.net.InetSocketAddress
import com.wajam.nrv.transport.Transport
import com.wajam.nrv.data.{OutMessage, MessageType, InMessage, Message}
import com.yammer.metrics.scala.Instrumented

/**
 * Protocol used to send and receive messages to remote nodes over a network
 */
abstract class Protocol(var name: String, messageRouter: ProtocolMessageListener) extends MessageHandler with Logging with Instrumented {

  private val sendingMessageFailure = metrics.meter("sendMessageFailure", "failure")
  val transport: Transport

  /**
   * Start the protocol and the transport layer below it.
   */
  def start()

  /**
   * Stop the protocol and the transport layer below it.
   */
  def stop()

  override def handleIncoming(action: Action, message: InMessage) {
    try {
      this.messageRouter.messageReceived(message)
    } catch {
      case e: ListenerException => {
        transport.sendResponse(message.attachments(Protocol.CONNECTION_KEY).asInstanceOf[Option[AnyRef]].get,
          generate(createErrorMessage(message, e, 404)),
          false)
      }
    }
  }

  override def handleOutgoing(action: Action, message: OutMessage) {
    message.protocolName = this.name
    message.attachments.getOrElse(Protocol.CONNECTION_KEY, None).asInstanceOf[Option[AnyRef]] match {
      case Some(channel) => {
        val response = generate(message)
        transport.sendResponse(channel,
          response,
          message.attachments.getOrElse(Protocol.CLOSE_AFTER, false).asInstanceOf[Boolean],
          (result: Option[Throwable]) => {
            result match {
              case Some(throwable) => {
                sendingMessageFailure.mark()
                warn("Could not send the response because of an error: {}.", throwable.toString)
              }
              case None =>
            }
          })
      }
      case None => {
        val node = message.destination(0).node
        val request = generate(message)
        System.err.println(message)
        transport.sendMessage(new InetSocketAddress(node.host, node.ports(name)),
          request,
          message.attachments.getOrElse(Protocol.CLOSE_AFTER, false).asInstanceOf[Boolean],
          (result: Option[Throwable]) => {
            result match {
              case Some(throwable) => {
                val response = new InMessage()
                message.copyTo(response)
                response.error = Some(new RuntimeException(throwable))
                response.function = MessageType.FUNCTION_RESPONSE

                handleIncoming(action, response, Unit => {})
              }
              case None =>
            }
          })
      }
    }
  }

  def transportMessageReceived(message: AnyRef, connectionInfo: Option[AnyRef]) {
    val inMessage = new InMessage
    inMessage.attachments.put(Protocol.CONNECTION_KEY, connectionInfo)
    try {
      val parsedMessage = parse(message)
      parsedMessage.copyTo(inMessage)
      handleIncoming(null, inMessage)
    } catch {
      case pe: ParsingException => {
        warn("Parsing exception: {}", pe)
        handleOutgoing(null, createErrorMessage(inMessage, pe, pe.code))
      }
      case e: Exception => {
        warn("Exception caught while processing a message from transport: {}", e)
        handleOutgoing(null, createErrorMessage(inMessage, e, 500))
      }
    }
  }

  /**
   * Parse the received message and convert it to a standard Message object.
   *
   * @param message The message received from the network
   * @return The standard Message object that represent the network message
   */
  def parse(message: AnyRef): Message

  /**
   * Generate a transport message from a standard Message object.
   *
   * @param message The standard Message object
   * @return The message to be sent of the network
   */
  def generate(message: Message): AnyRef

  /**
   * Create an Error message
   *
   * @param inMessage The message that triggered an error.
   * @param exception The exception
   * @return The error message to send
   */
  def createErrorMessage(inMessage: InMessage, exception: Exception, code: Int = 500): OutMessage = {
    val errorMessage = new OutMessage()
    inMessage.copyTo(errorMessage)
    errorMessage.code = code
    errorMessage.messageData = exception.getMessage
    errorMessage
  }
}

object Protocol {
  val CONNECTION_KEY = "connection"
  val CLOSE_AFTER = "close_after"
}

/**
 * Entity that will receive message from the protocol.
 */
trait ProtocolMessageListener {

  /**
   * Route the received message
   *
   * @param inMessage The received message
   */
  @throws(classOf[ListenerException])
  def messageReceived(inMessage: InMessage)
}

case class ParsingException(message: String, code: Int = 400) extends Exception(message)

case class ListenerException(message: String) extends Exception(message)
