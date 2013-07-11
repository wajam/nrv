package com.wajam.nrv.protocol

import com.wajam.nrv.cluster.{Node, LocalNode}
import com.wajam.nrv.data.Message
import com.wajam.nrv.service.Action

class NrvLocalOptimizedProtocol(name: String,
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

    val wasLocal = flags.getOrElse("isLocalBound", false).asInstanceOf[Boolean]

    if (wasLocal)
      localProtocol.parse(message, flags)
    else
      remoteProtocol.parse(message, flags)
  }

  def generate(message: Message, flags: Map[String, Any]): AnyRef = {

    val isLocal = flags.getOrElse("isLocalBound", false).asInstanceOf[Boolean]

    if (isLocal)
      localProtocol.generate(message, flags)
    else
      remoteProtocol.generate(message, flags)
  }

  def sendMessage(destination: Node, message: AnyRef, closeAfter: Boolean, flags: Map[String, Any], completionCallback: (Option[Throwable]) => Unit) {

    val isLocal = flags.getOrElse("isLocalBound", false).asInstanceOf[Boolean]

    val properSendMessage =
      if (isLocal)
        localProtocol.sendMessage _
      else
        remoteProtocol.sendMessage _

    properSendMessage(destination, message, closeAfter, flags, completionCallback)
  }

  def sendResponse(connection: AnyRef, message: AnyRef, closeAfter: Boolean, flags: Map[String, Any], completionCallback: (Option[Throwable]) => Unit) {

    val wasLocal = flags.getOrElse("isLocalBound", false).asInstanceOf[Boolean]

    val properSendResponse =
      if (wasLocal)
        localProtocol.sendResponse _
      else
        remoteProtocol.sendResponse _

    properSendResponse(connection, message, closeAfter, flags, completionCallback)
  }



}
