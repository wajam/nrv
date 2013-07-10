package com.wajam.nrv.protocol

import com.wajam.nrv.cluster.{Node, LocalNode}
import com.wajam.nrv.data.Message
import com.wajam.nrv.service.Action

class NrvLocalOptimizedTransport(name: String,
                                 optimizeLocalCall: Boolean,
                                 localNode: LocalNode,
                                 localProtocol: NrvMemoryProtocol,
                                 remoteProtocol: NrvBinaryProtocol) extends Protocol(name, localNode) {

  def start() {
    localProtocol.start()
    remoteProtocol.start()
  }

  def stop() {
    localProtocol.stop()
    remoteProtocol.stop()
  }

  private def isLocalBound(destination: Node): Boolean = {
    destination.protocolsSocketAddress(remoteProtocol.name).equals(localNode.protocolsSocketAddress(remoteProtocol.name))  &&
    destination.ports(remoteProtocol.name).equals(localNode.ports(remoteProtocol.name))
  }

  override def bindAction(action: Action) = {
    localProtocol.bindAction(action)
    remoteProtocol.bindAction(action)
  }

  def parse(message: AnyRef, connection: AnyRef): Message = {
    connection match {
      case Some(NrvMemoryProtocol.CONNECTION_KEY) => localProtocol.parse(message, connection)
      case _ => remoteProtocol.parse(message, connection)
    }
  }

  def generateMessage(message: Message, destination: Node): AnyRef = {
    val properGenerate =
      if (optimizeLocalCall && isLocalBound(destination))
        localProtocol.generateMessage _
      else
        remoteProtocol.generateMessage _

    properGenerate(message, destination)
  }

  def generateResponse(message: Message, connection: AnyRef): AnyRef = {
    val properGenerate = connection match {
      case NrvMemoryProtocol.CONNECTION_KEY => localProtocol.generateResponse _
      case _ => remoteProtocol.generateResponse _
    }

    properGenerate(message, connection)
  }

  def sendMessage(destination: Node, message: AnyRef, closeAfter: Boolean, completionCallback: (Option[Throwable]) => Unit) {
    val properSendMessage =
      if (optimizeLocalCall && isLocalBound(destination))
        localProtocol.sendMessage _
      else
        remoteProtocol.sendMessage _

    properSendMessage(destination, message, closeAfter, completionCallback)
  }

  def sendResponse(connection: AnyRef, message: AnyRef, closeAfter: Boolean, completionCallback: (Option[Throwable]) => Unit) {
    val properSendResponse = connection match {
      case NrvMemoryProtocol.CONNECTION_KEY => localProtocol.sendResponse _
      case _ => remoteProtocol.sendResponse _
    }

    properSendResponse(connection, message, closeAfter, completionCallback)
  }



}
