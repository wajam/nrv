package com.wajam.nrv.service

import com.wajam.nrv.data.{MessageType, OutMessage, InMessage}
import com.wajam.nrv.{RemoteException, UnavailableException}
import com.wajam.nrv.utils.Sync
import com.yammer.metrics.scala.Instrumented

/**
 * Action that binds a path to a callback. This is analogous to a RPC endpoint function,
 * but uses path to locale functions instead of functions name.
 */
class Action(var path: ActionPath, var implementation: ((InMessage) => Unit)) extends ActionSupport with Instrumented {
  def call(data: Map[String, Any]): Sync[InMessage] = {
    val sync = new Sync[InMessage]
    this.call(data, sync.done(_, _))
    sync
  }

  def call(data: Map[String, Any], onReply: ((InMessage, Option[Exception]) => Unit)) {
    this.call(new OutMessage(data, onReply))
  }

  def call(message: OutMessage) {
    this.handleOutgoingMessage(message)
  }

  protected[nrv] def matches(path: ActionPath) = this.path.matchesPath(path)._1

  protected[nrv] def start() {
    this.switchboard.start()
  }

  protected[nrv] def stop() {
    // TODO: find a way to stop switchboard
  }

  /**
   * Handles messages that needs to be sent to a remote node.
   * @param outMessage Sent message
   */
  protected[nrv] def handleOutgoingMessage(outMessage: OutMessage) {
    this.checkSupported()

    // initialize message
    outMessage.source = this.cluster.localNode
    outMessage.serviceName = this.service.name
    outMessage.path = this.path.buildPath(outMessage)
    outMessage.function = MessageType.FUNCTION_CALL

    // resolve endpoints
    this.resolver.handleOutgoing(this, outMessage)
    if (outMessage.destination.size == 0)
      throw new UnavailableException

    // add message to router (for response)
    this.switchboard.keepOutgoing(outMessage)

    this.protocol.handleOutgoing(this, outMessage)
  }

  /**
   * Handles messages received from a remote node.
   * @param inMessage Received messages
   */
  protected[nrv] def handleIncomingMessage(inMessage: InMessage) {
    this.switchboard.matchIncoming(inMessage, (outMessage) => {
      outMessage match {
        // it's a reply to a message
        case Some(originalMessage) =>
          originalMessage.handleReply(inMessage)

        // no original message, means that this is a new message
        case None => {

          // set the reply callback for this message
          inMessage.replyCallback = (responseMessage => {
            responseMessage.source = this.cluster.localNode
            responseMessage.serviceName = this.service.name
            responseMessage.path = inMessage.path
            responseMessage.function = MessageType.FUNCTION_RESPONSE
            responseMessage.rendezvous = inMessage.rendezvous

            // TODO: shouldn't be like that. Source may not be a member...
            responseMessage.destination = new Endpoints(Seq(new ServiceMember(0, inMessage.source)))

            this.protocol.handleOutgoing(this, responseMessage)
          })

          // handle the message, catch errors to throw them back to the caller
          try {
            this.implementation(inMessage)
          } catch {
            case ex: Exception => {
              val errMessage = new OutMessage
              errMessage.error = Some(new RemoteException(ex.getMessage))
              inMessage.reply(errMessage)
            }
          }
        }
      }
    })
  }
}
