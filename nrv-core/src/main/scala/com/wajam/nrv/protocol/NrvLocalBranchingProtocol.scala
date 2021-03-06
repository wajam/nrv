package com.wajam.nrv.protocol

import com.wajam.nrv.cluster.{Node, LocalNode}
import com.wajam.nrv.data.Message
import com.wajam.nrv.service.Action

/**
 * Meta protocol that fork in between to protocol a local one and a normal remote one according if the message
 * is bound to be local or not. It does he inverse forking as well,
 * i.e. it fork the right protocol according to the fork use to send the message.
 */
class NrvLocalBranchingProtocol(name: String,
                                localNode: LocalNode,
                                localProtocol: Protocol,
                                remoteProtocol: Protocol) extends Protocol(name, localNode) {

  // The names must match to avoid conflict
  require(name == localProtocol.name)
  require(name == remoteProtocol.name)

  def start() {
    localProtocol.start()
    remoteProtocol.start()
  }

  def stop() {
    localProtocol.stop()
    remoteProtocol.stop()
  }

  override def bindAction(action: Action) = {
    localProtocol.bindAction(action)
    remoteProtocol.bindAction(action)
  }

  def parse(message: AnyRef, flags: Map[String, Any]): Message = {

    val wasLocal = FlagReader.isLocalBound(flags)

    if (wasLocal)
      localProtocol.parse(message, flags)
    else
      remoteProtocol.parse(message, flags)
  }

  def generate(message: Message, flags: Map[String, Any]): AnyRef = {

    val isLocal = FlagReader.isLocalBound(flags)

    if (isLocal)
      localProtocol.generate(message, flags)
    else
      remoteProtocol.generate(message, flags)
  }

  def sendMessage(destination: Node, message: AnyRef, closeAfter: Boolean, flags: Map[String, Any], completionCallback: (Option[Throwable]) => Unit) {

    val isLocal = FlagReader.isLocalBound(flags)

    val properSendMessage =
      if (isLocal)
        localProtocol.sendMessage _
      else
        remoteProtocol.sendMessage _

    properSendMessage(destination, message, closeAfter, flags, completionCallback)
  }

  def sendResponse(connection: AnyRef, message: AnyRef, closeAfter: Boolean, flags: Map[String, Any], completionCallback: (Option[Throwable]) => Unit) {

    val wasLocal = FlagReader.isLocalBound(flags)

    val properSendResponse =
      if (wasLocal)
        localProtocol.sendResponse _
      else
        remoteProtocol.sendResponse _

    properSendResponse(connection, message, closeAfter, flags, completionCallback)
  }
}
