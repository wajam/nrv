package com.wajam.nrv.protocol

import com.wajam.nrv.cluster.LocalNode
import java.net.InetSocketAddress
import com.wajam.nrv.data.Message

class NrvLocalOptimizedTransport(name: String,
                                 localNode: LocalNode,
                                 localProtocol: Protocol,
                                 remoteProtocol: Protocol) extends Protocol(name, localNode) {

  def start() {}

  def stop() {}

  def parse(message: AnyRef): Message = null

  def generate(message: Message): AnyRef = null

  def sendMessage(destination: InetSocketAddress, message: AnyRef, closeAfter: Boolean, completionCallback: (Option[Throwable]) => Unit) {}

  def sendResponse(connection: AnyRef, message: AnyRef, closeAfter: Boolean, completionCallback: (Option[Throwable]) => Unit) {}
}
