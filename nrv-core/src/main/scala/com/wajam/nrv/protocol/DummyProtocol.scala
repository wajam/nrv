package com.wajam.nrv.protocol

import com.wajam.nrv.service.Action
import com.wajam.nrv.cluster.Cluster
import com.wajam.nrv.data.{OutMessage, Message, InMessage}

/**
 * Loopback protocol that always send messages to local process
 */
class DummyProtocol(cluster: Cluster, name: String) extends Protocol(name, cluster) {

  override val transport = null

  override def handleOutgoing(action: Action, message: OutMessage) {
    val newMessage = new InMessage()
    message.copyTo(newMessage)
    cluster.routeIncoming(newMessage)
  }

  def start() {}

  def stop() {}

  def parse(message: AnyRef): Message = null

  def generate(message: Message): AnyRef = null
}