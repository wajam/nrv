package com.wajam.nrv.service

import com.yammer.metrics.scala.Instrumented
import java.util.concurrent.TimeUnit
import com.wajam.commons.Logging
import com.wajam.nrv.{RemoteException, UnavailableException}
import com.wajam.nrv.data._
import scala.concurrent.{Future, Promise}
import com.wajam.nrv.protocol.Protocol

/**
 * Action that binds a path to a callback. This is analogous to a RPC endpoint function,
 * but uses path to locale functions instead of functions name.
 */
class Action(val path: ActionPath,
             val implementation: ((InMessage) => Unit),
             val method: ActionMethod = ActionMethod.ANY,
             actionSupportOptions: ActionSupportOptions = new ActionSupportOptions())
  extends ActionSupport with Instrumented with Logging {

  lazy val fullPath = this.protocol.name + "://" + this.service.name + this.path
  lazy val metricsPath = this.protocol.name + "-" + this.service.name + this.path.replace("/", "-").replace(":", "+")

  private lazy val msgInMeter = metrics.meter("message-in", "messages-in", metricsPath)
  private lazy val msgOutMeter = metrics.meter("message-out", "messages-out", metricsPath)
  private lazy val msgOutUnavailableRecipient = metrics.meter("message-out-unavailable-recipient", "message-out-unavailable-recipient", metricsPath)

  private lazy val responseInUnexpected = metrics.meter("response-in-unexpected", "response-in-unexpected", metricsPath)
  private lazy val responseInProcessError = metrics.meter("response-in-process-error", "response-in-process-error", metricsPath)
  private lazy val requestInProcessError = metrics.meter("request-in-process-error", "request-in-process-error", metricsPath)

  private lazy val msgReplyTime = metrics.timer("reply-time", metricsPath)
  private lazy val executeTime = metrics.timer("execute-time", metricsPath)

  // overrides defaults with passed options
  applySupportOptions(actionSupportOptions)

  def call(params: Iterable[(String, MValue)]): Future[InMessage] = {
    call(params, null, null)
  }

  def call(params: Iterable[(String, MValue)],
           responseTimeout: Long): Future[InMessage] = {
    call(params, null, null, responseTimeout)
  }

  def call(params: Iterable[(String, MValue)],
           meta: Iterable[(String, MValue)],
           data: Any): Future[InMessage] = {
    call(params, meta, data, responseTimeout)
  }

  def call(params: Iterable[(String, MValue)],
           meta: Iterable[(String, MValue)],
           data: Any,
           responseTimeout: Long): Future[InMessage] = {
    call(params, meta, data, responseTimeout, None)
  }

  def call(params: Iterable[(String, MValue)],
           meta: Iterable[(String, MValue)],
           data: Any,
           responseTimeout: Long,
           protocolName: Option[String]): Future[InMessage] = {
    val p = Promise[InMessage]()
    def complete(msg: InMessage, optException: Option[Exception]) {
      optException match {
        case Some(e) => p.failure(e)
        case None => p.success(msg)
      }
    }
    call(params, complete, meta, data, responseTimeout, protocolName)
    p.future
  }

  @deprecated("Use call methods returning Future", "September 2013")
  def call(params: Iterable[(String, MValue)],
           onReply: ((InMessage, Option[Exception]) => Unit),
           meta: Iterable[(String, MValue)] = null,
           data: Any = null,
           responseTimeout: Long = responseTimeout,
           protocolName: Option[String] = None) {
    val message = new OutMessage(params, meta, data, onReply = onReply, responseTimeout = responseTimeout)
    for(name <- protocolName) {
      message.protocolName = name
    }
    call(message)
  }

  def call(message: OutMessage) {
    this.checkSupported()

    message.method = method
    message.function = MessageType.FUNCTION_CALL
    this.callOutgoingHandlers(message)
  }

  def matches(path: ActionPath, method: ActionMethod) = {
    (this.path.matchesPath(path)._1 && this.method.matchMethod(method))
  }

  protected[nrv] def start() {
    this.checkSupported()
    this.protocol.bindAction(this)
    this.supportedProtocols.foreach(p => p.bindAction(this))
    this.switchboard.start()
  }

  protected[nrv] def stop() {
    this.switchboard.stop()
  }

  /**
   * Handles messages that needs to be sent to a remote node by calling
   * message handlers one by one
   * @param outMessage Sent message
   */
  protected[nrv] def callOutgoingHandlers(outMessage: OutMessage) {
    this.msgOutMeter.mark()

    // initialize message
    outMessage.source = this.cluster.localNode
    outMessage.serviceName = this.service.name
    outMessage.path = this.path.buildPath(outMessage.parameters)
    outMessage.sentTime = System.currentTimeMillis()

    // resolve endpoints
    this.resolver.handleOutgoing(this, outMessage, ()  => {
      if (outMessage.destination.selectedReplicas.size == 0) {
        msgOutUnavailableRecipient.mark()
        throw new UnavailableException
      }

      // Store current trace context in message attachment for the trace filter
      if (tracer.currentContext.isDefined) {
        outMessage.attachments(TraceHeader.OriginalContext) = tracer.currentContext.get
      }

      this.switchboard.handleOutgoing(this, outMessage, () => {
        TraceFilter.handleOutgoing(this, outMessage, () => {
          this.consistency.handleOutgoing(this, outMessage, () => {
            this.resolveOutProtocol(outMessage).handleOutgoing(this, outMessage, () => {
              outMessage.sentTime = System.currentTimeMillis()
            })
          })
        })
      })
    })
  }

  /**
   * Handles messages received from a remote node, calls handlers
   * one by one
   * @param fromMessage Received messages
   */
  protected[nrv] def callIncomingHandlers(fromMessage: InMessage) {
    this.msgInMeter.mark()

    this.resolver.handleIncoming(this, fromMessage, () => {
      this.switchboard.handleIncoming(this, fromMessage, () => {
        TraceFilter.handleIncoming(this, fromMessage, () => {

          // Generate the reply callback, if applicable.
          fromMessage.function match {
            case MessageType.FUNCTION_CALL =>
              // set the reply callback for this message
              fromMessage.replyCallback = (intoMessage => {
                this.generateResponseMessage(fromMessage, intoMessage)
                this.callOutgoingHandlers(intoMessage)
              })
            case _ =>
          }

          this.consistency.handleIncoming(this, fromMessage, () => {
            fromMessage.function match {
              case MessageType.FUNCTION_CALL =>
                // handle the message, catch errors to throw them back to the caller
                try {
                  this.executeTime.time {
                    extractParamsFromPath(fromMessage, fromMessage.path)
                    this.implementation(fromMessage)
                  }
                } catch {
                  case ex: Exception => {
                    requestInProcessError.mark()
                    warn("Got an exception calling implementation", ex)
                    try {
                      fromMessage.replyWithError(new RemoteException("Exception calling action implementation", ex))
                    } catch {
                      case e: Exception => log.error("Could not send error message back to caller.", e)
                    }
                  }
                }

              case MessageType.FUNCTION_RESPONSE =>
                fromMessage.matchingOutMessage match {
                  // it's a reply to a message
                  case Some(originalMessage) =>
                    this.msgReplyTime.update(System.currentTimeMillis() - originalMessage.sentTime, TimeUnit.MILLISECONDS)
                    try {
                      originalMessage.handleReply(fromMessage)
                    } catch {
                      case e: UnavailableException => {
                        responseInProcessError.mark()
                        debug("Got an UnavailableException calling reply callback", e)
                      }
                      case ex: Exception => {
                        responseInProcessError.mark()
                        warn("Got an exception calling reply callback", ex)
                      }
                    }
                  case None => {
                    responseInUnexpected.mark()
                    debug("Response with no matching original message received")
                  }
                }
            }
          })
        })
      })
    })
  }

  protected[nrv] def generateResponseMessage(fromMessage: Message, intoMessage: Message) {
    intoMessage.source = this.cluster.localNode
    intoMessage.serviceName = this.service.name
    intoMessage.timestamp = fromMessage.timestamp
    intoMessage.path = fromMessage.path
    intoMessage.method = fromMessage.method
    intoMessage.function = MessageType.FUNCTION_RESPONSE
    intoMessage.rendezvousId = fromMessage.rendezvousId
    intoMessage.attachments ++= fromMessage.attachments

    extractParamsFromPath(intoMessage, fromMessage.path)

    // TODO: shouldn't be like that. Source may not be a member...
    intoMessage.destination = new Endpoints(Seq(new Shard(0, Seq(new Replica(0, fromMessage.source)))))
  }

  private def extractParamsFromPath(intoMessage: Message, path: String) {
    val (_, params) = this.path.matchesPath(path)
    intoMessage.parameters ++= params
  }

  private def resolveOutProtocol(outMessage: OutMessage): Protocol = {
    //find the protocol to use to send this message with fallback to default protocol
    supportedProtocols.find(p => p.name == outMessage.protocolName).getOrElse(protocol)
  }
}

/**
 * Utilities method to ease action declaration
 */
object Action {
  def createAction(path: ActionPath, method: ActionMethod, implementation: ((InMessage) => Unit)): Action = {
    new Action(path, implementation, method)
  }

  def forPath(path: String): ActionPath = {
    path
  }

  def forMethod(method: String): ActionMethod = {
    method
  }
}
