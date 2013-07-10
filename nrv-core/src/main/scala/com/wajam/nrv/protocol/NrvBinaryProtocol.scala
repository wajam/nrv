package com.wajam.nrv.protocol

import com.wajam.nrv.transport.nrv.NrvNettyTransport
import com.wajam.nrv.data.Message
import com.google.common.primitives.UnsignedBytes
import java.nio.ByteBuffer
import com.wajam.nrv.data.serialization.NrvProtobufSerializer
import com.wajam.nrv.cluster.{Node, LocalNode}

class NrvBinaryProtocol(name: String,
                        localNode: LocalNode,
                        idleConnectionTimeoutMs: Long,
                        maxConnectionPoolSize: Int) extends Protocol(name, localNode)  {

  val transport = new NrvNettyTransport(localNode.listenAddress, localNode.ports(name),
    this, idleConnectionTimeoutMs, maxConnectionPoolSize)

  protected val resolveCodec = (msg: Message) => {

    val optAction = this.resolveAction(msg.serviceName, msg.actionURL.path, msg.method)
    optAction.get.nrvCodec
  }

  def start() {
    transport.start()
  }

  def stop() {
    transport.stop()
  }

  def parse(message: AnyRef, connection: AnyRef): Message = {
    val bytes = message.asInstanceOf[Array[Byte]]

    val magicByte: Int = UnsignedBytes.toInt(bytes(0))

    if (magicByte == (UnsignedBytes.toInt(NrvBinaryProtocol.NrvMagicByte)))
      parseProtobuf(bytes)
    else
      throw new RuntimeException("The magic byte was not recognized.")
  }

  def generateMessage(message: Message, destination: Node): AnyRef = {
    generateProtobuf(message)
  }

  def generateResponse(message: Message, connection: AnyRef): AnyRef = {
    generateProtobuf(message)
  }

  private def parseProtobuf(message: Array[Byte]): Message = {

    if (message.length < 1)
      throw new IllegalArgumentException("message needs at least one byte of data")

    val messageLength = message.length - 1

    val bytes = new Array[Byte](messageLength)

    // Get bytes without the magic byte
    val buffer = ByteBuffer.wrap(message)
    buffer.get()
    buffer.get(bytes)

    NrvProtobufSerializer.deserializeMessage(bytes, resolveCodec)
  }

  private def generateProtobuf(message: Message): Array[Byte] = {

    val bytes = NrvProtobufSerializer.serializeMessage(message, resolveCodec)

    val messageLength = bytes.length + 1

    val buffer = ByteBuffer.allocate(messageLength)

    buffer.put(NrvBinaryProtocol.NrvMagicByte)

    buffer.put(bytes)

    buffer.array()
  }

  def sendMessage(destination: Node,
                  message: AnyRef,
                  closeAfter: Boolean,
                  completionCallback: (Option[Throwable]) => Unit) {
    transport.sendMessage(destination.protocolsSocketAddress(name), message, closeAfter, completionCallback)
  }

  def sendResponse(connection: AnyRef,
                   message: AnyRef,
                   closeAfter: Boolean,
                   completionCallback: (Option[Throwable]) => Unit) {
    transport.sendResponse(connection, message, closeAfter, completionCallback)
  }
}

object NrvBinaryProtocol {
  private val NrvMagicByte : Byte = (0xF2).toByte
}
