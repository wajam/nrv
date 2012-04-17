package com.wajam.nrv.protocol

import com.wajam.nrv.service.Action
import com.wajam.nrv.data.{Message, InMessage}
import com.wajam.nrv.cluster.Cluster

/**
 * Loopback protocol that always send messages to local process
 */
class DummyProtocol(cluster: Cluster, name: String) extends Protocol(name, cluster) {
  override def handleOutgoing(action: Action, message: Message) {
    val newMessage = new InMessage()
    message.copyTo(newMessage)
    cluster.routeIncoming(newMessage)
  }

  def start() = null

  def stop() = null

  def parse(message: AnyRef): Message = null

  def generate(message: Message): AnyRef = null
}