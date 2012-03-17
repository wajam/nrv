package com.appaquet.nrv.protocol

import com.appaquet.nrv.codec.Codec
import com.appaquet.nrv.data.Message
import com.appaquet.nrv.service.{ActionUrl, Action, MessageHandler}
import com.appaquet.nrv.cluster.Cluster
import com.appaquet.nrv.Logging

/**
 * Protocol used to send and receive messages to remote nodes over a network
 */
abstract class Protocol(var name: String, var cluster:Cluster, var codec: Codec) extends MessageHandler with Logging {
  def encodeMessage(message: Message): Array[Byte] = {
    codec.encode(message)
  }

  def decodeMessage(data: Array[Byte]): Message = {
    codec.decode(data)
  }

  def start()

  def stop()

  def handleIncoming(action: Action, message: Message) {
    val url = new ActionUrl(message.serviceName, message.path)
    val resolvedAction = this.cluster.getAction(url)
   
    if (resolvedAction == null)
      error("No such service or invalid path: {}", url)
  }
}
