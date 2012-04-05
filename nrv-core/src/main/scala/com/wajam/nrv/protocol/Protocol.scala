package com.wajam.nrv.protocol

import com.wajam.nrv.codec.Codec
import com.wajam.nrv.data.Message
import com.wajam.nrv.cluster.Cluster
import com.wajam.nrv.Logging
import com.wajam.nrv.service.{Action, MessageHandler}

/**
 * Protocol used to send and receive messages to remote nodes over a network
 */
abstract class Protocol(var name: String, var cluster: Cluster, var codec: Codec) extends MessageHandler with Logging {
  def encodeMessage(message: Message): Array[Byte] = {
    codec.encode(message)
  }

  def decodeMessage(data: Array[Byte]): Message = {
    codec.decode(data)
  }

  def start()

  def stop()

  def handleIncoming(action: Action, message: Message) {
    this.cluster.route(message)
  }
}
