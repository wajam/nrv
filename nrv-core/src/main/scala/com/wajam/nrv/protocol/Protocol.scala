package com.wajam.nrv.protocol

import com.wajam.nrv.{RouteException, Logging}
import com.wajam.nrv.service.{ActionMethod, Service, MessageHandler, Action}
import com.wajam.nrv.transport.Transport
import com.wajam.nrv.data.{OutMessage, MessageType, InMessage, Message}
import com.yammer.metrics.scala.Instrumented
import org.scalatest.matchers.MustMatchers.AnyRefMustWrapper

/**
 * Protocol used to send and receive messages to remote nodes over a network
 */
abstract class Protocol(var name: String) extends MessageHandler with Logging with Instrumented {

  private val sendingResponseFailure = metrics.meter("sendResponseFailure", "failure")
  private val parsingError = metrics.meter("parsing-error", "error")
  private val routingError = metrics.meter("routing-error", "error")
  private val receptionError = metrics.meter("reception-error", "error")

  val transport: Transport
  var services = Map[String, Service]()


  /**
   * Start the protocol and the transport layer below it.
   */
  def start()

  /**
   * Stop the protocol and the transport layer below it.
   */
  def stop()

  def bindAction(action: Action) {
    services += (action.service.name -> action.service)
  }

  protected def resolveAction(serviceName: String, path: String, method: ActionMethod) : Option[Action] = {

    services.get(serviceName) match {
      case Some(service) =>
        // a service by the name is found, use it directly
        service.findAction(path, method)

      case None =>
        // else we reduce services, finding the one with the action that can be called
        services.values.foldLeft[Option[Action]](None)((current, service) => current match {
          case Some(currentAction) => current
          case None => service.findAction(path, method)
        })
    }
  }

  override def handleIncoming(action: Action, message: InMessage) {
    try {

      val optAction = resolveAction(message.serviceName, message.actionURL.path, message.method)

      optAction match {
        case Some(foundAction) => foundAction.callIncomingHandlers(message)
        case None =>
          error("Couldn't find services/action for received message {}", message)
          throw new RouteException("No route found for received message " + message.toString)
      }

    } catch {
      case e: RouteException => {
        routingError.mark()
        handleIncomingMessageError(e, message.attachments(Protocol.CONNECTION_KEY).asInstanceOf[Option[AnyRef]])
      }
    }
  }

  private def handleResponse(channel: AnyRef, message: OutMessage)
  {
    var optResponse: Option[AnyRef] = None

    try {
      optResponse = Some(generate(message))
    }
    catch {
      case e: Exception => {
        sendingResponseFailure.mark()
        log.error("Could not send response because it cannot be constructed: error = {}.",
          e.toString)
      }
    }

    for(response <- optResponse) {
      transport.sendResponse(channel,
        response,
        message.attachments.getOrElse(Protocol.CLOSE_AFTER, false).asInstanceOf[Boolean],
        (result: Option[Throwable]) => {
          result match {
            case Some(throwable) => {
              sendingResponseFailure.mark()
              log.debug("Could not send the response because of an error: response = {}, error = {}.",
                message, throwable.toString)
            }
            case None =>
          }
        })
    }
  }

  private def handleRequest(action: Action, message: OutMessage) {
    var optRequest: Option[AnyRef] = None

    try {
      optRequest = Some(generate(message))
    }
    catch {
      case e: Exception => {
        log.error("Could not send request because it cannot be constructed: error = {}.",
          e.toString)
      }
    }

    for (request <- optRequest) {
      for (replica <- message.destination.selectedReplicas) {
        val node = replica.node

        transport.sendMessage(node.protocolsSocketAddress(name),
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

  override def handleOutgoing(action: Action, message: OutMessage) {
    message.protocolName = this.name

    message.attachments.getOrElse(Protocol.CONNECTION_KEY, None).asInstanceOf[Option[AnyRef]] match {

      case Some(channel) =>  handleResponse(channel, message)
      case None => handleRequest(action, message)
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
        parsingError.mark()
        warn("Parsing exception: {}", pe)
        handleIncomingMessageError(pe, connectionInfo)
      }
      case e: Exception => {
        receptionError.mark()
        warn("Exception caught while processing a message from transport", e)
        handleIncomingMessageError(e, connectionInfo)
      }
    }
  }

  protected def handleIncomingMessageError(exception: Exception, connectionInfo: Option[AnyRef]) {}

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
}

object Protocol {
  val CONNECTION_KEY = "connection"
  val CLOSE_AFTER = "close_after"
}

case class ParsingException(message: String, code: Int = 400) extends Exception(message)

case class ListenerException(message: String) extends Exception(message)
