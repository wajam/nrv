package com.appaquet.nrv.protocol

import com.appaquet.nrv.codec.JavaSerializeCodec
import com.appaquet.nrv.service.Action
import com.appaquet.nrv.data.{Message, InRequest}
import com.appaquet.nrv.cluster.Cluster

/**
 * Loopback protocol that always send messages to local process
 */
class DummyProtocol(cluster:Cluster, name: String) extends Protocol(name, cluster, new JavaSerializeCodec) {
  def handleOutgoing(action: Action, message: Message) {
    val newRequest = new InRequest()
    message.copyTo(newRequest)
    action.handleIncomingRequest(newRequest)
  }

  def start() = null

  def stop() = null
}