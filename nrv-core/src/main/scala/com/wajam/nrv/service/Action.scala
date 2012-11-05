package com.wajam.nrv.service

import com.wajam.nrv.utils.{Promise, Future}
import com.yammer.metrics.scala.Instrumented
import java.util.concurrent.TimeUnit
import scala.Unit
import com.wajam.nrv.{Logging, RemoteException, UnavailableException}
import com.wajam.nrv.data._
import scala.Some
import com.wajam.nrv.consistency.Consistency

/**
 * Action that binds a path to a callback. This is analogous to a RPC endpoint function,
 * but uses path to locale functions instead of functions name.
 */
class Action(val path: ActionPath,
             val implementation: ((InMessage) => Unit),
             val method: ActionMethod = ActionMethod.ANY,
             consistency: Option[Consistency] = None,
             defaultTimeout: Long = 1000)
  extends ActionSupport with Instrumented with Logging {

  // override protocol, resolver if defined
  applySupport(consistency = consistency)

  lazy val fullPath = this.protocol.name + "://" + this.service.name + this.path
  lazy val metricsPath = this.protocol.name + "-" + this.service.name + this.path.replace("/", "-").replace(":", "+")

  private lazy val msgInMeter = metrics.meter("message-in", "messages-in", metricsPath)
  private lazy val msgOutMeter = metrics.meter("message-out", "messages-out", metricsPath)
  private lazy val msgReplyTime = metrics.timer("reply-time", metricsPath)
  private lazy val executeTime = metrics.timer("execute-time", metricsPath)

  def call(params: Iterable[(String, Any)],
           meta: Iterable[(String, Any)],
           data: Any): Future[InMessage] = {
    call(params, meta, data, defaultTimeout)
  }

  def call(params: Iterable[(String, Any)],
           meta: Iterable[(String, Any)],
           data: Any,
           timeout: Long): Future[InMessage] = {
    val p = Promise[InMessage]
    this.call(params, p.complete(_, _), meta, data, timeout)
    p.future
  }

  def call(params: Iterable[(String, Any)],
           onReply: ((InMessage, Option[Exception]) => Unit),
           meta: Iterable[(String, Any)] = null,
           data: Any = null,
           timeout: Long = defaultTimeout) {
    this.call(new OutMessage(params, meta, data, onReply = onReply, timeoutTime = timeout))
  }

  def call(message: OutMessage) {
    this.checkSupported()

    message.function = MessageType.FUNCTION_CALL
    this.callOutgoingHandlers(message)
  }

  def matches(path: ActionPath, method: ActionMethod) = {
    (this.path.matchesPath(path)._1 && this.method.matchMethod(method))
  }

  protected[nrv] def start() {
    this.checkSupported()
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
    this.resolver.handleOutgoing(this, outMessage, _ => {
      if (outMessage.destination.selectedReplicas.size == 0)
        throw new UnavailableException

      // Store current trace context in message attachment for the trace filter
      if (tracer.currentContext.isDefined) {
        outMessage.attachments(TraceHeader.OriginalContext) = tracer.currentContext.get
      }

      this.switchboard.handleOutgoing(this, outMessage, _ => {
        TraceFilter.handleOutgoing(this, outMessage, _ => {
          this.protocol.handleOutgoing(this, outMessage, _ => {
            outMessage.sentTime = System.currentTimeMillis()
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

    this.resolver.handleIncoming(this, fromMessage, _ => {
      this.switchboard.handleIncoming(this, fromMessage, _ => {
        TraceFilter.handleIncoming(this, fromMessage, _ => {
          fromMessage.function match {

            // function call
            case MessageType.FUNCTION_CALL =>
              // set the reply callback for this message
              fromMessage.replyCallback = (intoMessage => {
                this.generateResponseMessage(fromMessage, intoMessage)
                this.callOutgoingHandlers(intoMessage)
              })

              // handle the message, catch errors to throw them back to the caller
              try {
                this.executeTime.time {
                  extractParamsFromPath(fromMessage, fromMessage.path)
                  this.implementation(fromMessage)
                }
              } catch {
                case ex: Exception => {
                  warn("Got an exception calling implementation", ex)
                  try {
                    fromMessage.replyWithError(new RemoteException("Exception calling action implementation", ex))
                  } catch {
                    case e: Exception => log.error("Could not send error message back to caller.", e)
                  }
                }
              }


            // it's a reply to a message
            case MessageType.FUNCTION_RESPONSE =>
              fromMessage.matchingOutMessage match {
                // it's a reply to a message
                case Some(originalMessage) =>
                  this.msgReplyTime.update(System.currentTimeMillis() - originalMessage.sentTime, TimeUnit.MILLISECONDS)
                  try {
                    originalMessage.handleReply(fromMessage)
                  } catch {
                    case ex: Exception => {
                      warn("Got an exception calling reply callback", ex)
                    }
                  }
                case None =>
                  warn("Response with no matching original message received")
              }
          }
        })
      })
    })
  }

  protected[nrv] def generateResponseMessage(fromMessage: Message, intoMessage: Message) {
    intoMessage.source = this.cluster.localNode
    intoMessage.serviceName = this.service.name
    intoMessage.path = fromMessage.path
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
