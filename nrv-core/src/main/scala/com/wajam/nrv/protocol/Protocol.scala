package com.wajam.nrv.protocol

import com.wajam.nrv.{RouteException, Logging}
import com.wajam.nrv.service.{ActionMethod, Service, MessageHandler, Action}
import com.wajam.nrv.transport.Transport
import com.wajam.nrv.data.{OutMessage, MessageType, InMessage, Message}
import com.yammer.metrics.scala.Instrumented
import com.wajam.nrv.cluster.{Node, LocalNode}
import java.net.InetSocketAddress

/**
 * Protocol used to send and receive messages to remote nodes over a network
 */
abstract class Protocol(val name: String,
                        val localNode: LocalNode) extends MessageHandler with Logging with Instrumented {

  private val sendingResponseFailure = metrics.meter("sendResponseFailure", "failure")
  private val parsingError = metrics.meter("parsing-error", "error")
  private val routingError = metrics.meter("routing-error", "error")
  private val receptionError = metrics.meter("reception-error", "error")

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

  protected[nrv] def resolveAction(serviceName: String, path: String, method: ActionMethod) : Option[Action] = {

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

  private def guardedGenerate(generate: () => AnyRef): Either[Throwable, AnyRef] = {
    try {
      Right(generate())
    }
    catch {
      case e: Exception => Left(e)
    }
  }

  private def isMessageLocalBound(destination: Node): Boolean = {
    destination.protocolsSocketAddress(name).equals(localNode.protocolsSocketAddress(name))  &&
      destination.ports(name).equals(localNode.ports(name))
  }

  private def handleOutgoingResponse(connectionInfo: AnyRef, message: OutMessage) {

    val flags =
      message.attachments.getOrElse(Protocol.FLAGS, Map[String, Any]()).asInstanceOf[Map[String, Any]]

    guardedGenerate(() => generate(message, flags)) match {
      case Left(e) => {
        sendingResponseFailure.mark()
        log.error("Could not send response because it cannot be constructed: error = {}.", e.toString)
      }
      case Right(response) => {
        sendResponse(connectionInfo,
          response,
          message.attachments.getOrElse(Protocol.CLOSE_AFTER, false).asInstanceOf[Boolean],
          flags,
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
  }

  private def handleOutgoingRequest(action: Action, message: OutMessage) {

    for (replica <- message.destination.selectedReplicas) {
      val node = replica.node

      val flags =
          Map((flagReader.F_IS_LOCAL_BOUND -> isMessageLocalBound(node)))

      guardedGenerate(() => generate(message, flags)) match {
        case Left(e) => {
          log.error("Could not send request because it cannot be constructed: error = {}.", e.toString)
        }
        case Right(request) => {

          sendMessage(node,
            request,
            message.attachments.getOrElse(Protocol.CLOSE_AFTER, false).asInstanceOf[Boolean],
            flags,
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
  }

  override def handleOutgoing(action: Action, message: OutMessage) {
    message.protocolName = this.name

    message.attachments.getOrElse(Protocol.CONNECTION_KEY, None).asInstanceOf[Option[AnyRef]] match {

      case Some(channel) =>  handleOutgoingResponse(channel, message)
      case None => handleOutgoingRequest(action, message)
    }
  }

  def transportMessageReceived(message: AnyRef, connectionInfo: Option[AnyRef], flags: Map[String, Any]) {
    val inMessage = new InMessage
    inMessage.attachments.put(Protocol.CONNECTION_KEY, connectionInfo)
    inMessage.attachments.put(Protocol.FLAGS, flags)

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
  def parse(message: AnyRef, flags: Map[String, Any] = Map()): Message

  /**
   * Generate a transport message from a standard Message object.
   *
   * @param message The standard Message object
   * @return The message to be sent of the network
   */
  def generate(message: Message, flags: Map[String, Any] = Map()): AnyRef

  /**
   * Send a message on the transport layer.
   *
   * @param destination Destination's address
   * @param message The message to send
   * @param closeAfter Tells the transport layer to close or not the connection after the message has been sent
   * @param completionCallback Callback executed once the message has been sent or when a failure occured
   */
  def sendMessage(destination: Node,
                  message: AnyRef,
                  closeAfter:Boolean,
                  flags: Map[String, Any] = Map(),
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
                   flags: Map[String, Any] = Map(),
                   completionCallback: Option[Throwable] => Unit = (_) => {})

  protected val flagReader = new FlagReader()

  protected class FlagReader {

    val F_IS_LOCAL_BOUND = "isLocalBound"

    def isLocalBound(flags: Map[String, Any]): Boolean = {
      flags.getOrElse(F_IS_LOCAL_BOUND, false).asInstanceOf[Boolean]
    }
  }
}

object Protocol {
  val CONNECTION_KEY = "connection"
  val CLOSE_AFTER = "close_after"
  val FLAGS = "flags"
}

case class ParsingException(message: String, code: Int = 400) extends Exception(message)

case class ListenerException(message: String) extends Exception(message)
