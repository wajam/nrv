package com.wajam.nrv.protocol

import com.wajam.nrv.codec.JavaSerializeCodec
import com.wajam.nrv.service.Action
import com.wajam.nrv.data.{Message, InRequest}
import com.wajam.nrv.cluster.Cluster

/**
 * Loopback protocol that always send messages to local process
 */
class DummyProtocol(cluster: Cluster, name: String) extends Protocol(name, cluster) {
  override def handleOutgoing(action: Action, message: Message) {
    val newRequest = new InRequest()
    message.copyTo(newRequest)
    cluster.route(newRequest)
  }

  def start() = null

  def stop() = null

  def parse(message: AnyRef) : Message = null

  def generate(message: Message) : AnyRef = null
}