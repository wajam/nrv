package com.wajam.nrv.service

import com.wajam.nrv.utils.Sync
import com.yammer.metrics.scala.Instrumented
import java.util.concurrent.TimeUnit
import com.wajam.nrv.data.{Message, MessageType, OutMessage, InMessage}
import scala.Unit
import com.wajam.nrv.{Logging, RemoteException, UnavailableException}

/**
 * Action that binds a path to a callback. This is analogous to a RPC endpoint function,
 * but uses path to locale functions instead of functions name.
 */

class Action(var path: ActionPath,
             var implementation: ((InMessage) => Unit),
             var method: ActionMethod = ActionMethod.ANY)
  extends ActionSupport with Instrumented with Logging {

  private val msgInMeter = metrics.meter("message-in", "messages-in", this.path.replace(":","_"))
  private val msgOutMeter = metrics.meter("message-out", "messages-out", this.path.replace(":","_"))
  private val msgReplyTime = metrics.timer("reply-time", this.path.replace(":", "_"))
  private val executeTime = metrics.timer("execute-time", this.path.replace(":", "_"))

  def call(params: Iterable[(String, Any)],
           meta: Iterable[(String, Any)],
           data: Any): Sync[InMessage] = {
    val sync = new Sync[InMessage]
    this.call(params, sync.done(_, _), meta, data)
    sync
  }

  def call(params: Iterable[(String, Any)],
           onReply: ((InMessage, Option[Exception]) => Unit),
           meta: Iterable[(String, Any)] = null,
           data: Any = null) {
    this.call(new OutMessage(params, meta, data, onReply))
  }

  def call(message: OutMessage) {
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
      if (outMessage.destination.size == 0)
        throw new UnavailableException

      this.switchboard.handleOutgoing(this, outMessage, _ => {
        this.protocol.handleOutgoing(this, outMessage, _ => {
          outMessage.sentTime = System.currentTimeMillis()
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

    this.switchboard.handleIncoming(this, fromMessage, Unit => {

      fromMessage.matchingOutMessage match {
        // it's a reply to a message
        case Some(originalMessage) =>
          this.msgReplyTime.update(System.currentTimeMillis() - originalMessage.sentTime, TimeUnit.MILLISECONDS)
          originalMessage.handleReply(fromMessage)

        // no original message, means that this is a new message
        case None => {

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
              debug("Got an exeception calling implementation", ex)
              val errMessage = new OutMessage
              errMessage.error = Some(new RemoteException(ex.getMessage))
              try {
                fromMessage.reply(errMessage)
              } catch {
                case e: Exception => log.error("Could not send error message back to caller.", e)
              }
            }
          }
        }
      }
    })
  }

  protected[nrv] def generateResponseMessage(fromMessage:Message, intoMessage:Message) {
    intoMessage.source = this.cluster.localNode
    intoMessage.serviceName = this.service.name
    intoMessage.path = fromMessage.path
    intoMessage.function = MessageType.FUNCTION_RESPONSE
    intoMessage.rendezvousId = fromMessage.rendezvousId
    intoMessage.attachments ++= fromMessage.attachments

    extractParamsFromPath(intoMessage, fromMessage.path)

    // TODO: shouldn't be like that. Source may not be a member...
    intoMessage.destination = new Endpoints(Seq(new ServiceMember(0, fromMessage.source)))
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
