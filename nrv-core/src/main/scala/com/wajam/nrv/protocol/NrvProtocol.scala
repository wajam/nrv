package com.wajam.nrv.protocol

import java.nio.ByteBuffer
import codec.MessageJavaSerializeCodec
import com.wajam.nrv.data.Message
import com.wajam.nrv.transport.nrv.NrvNettyTransport
import com.wajam.nrv.cluster.LocalNode
import com.wajam.nrv.data.serialization.NrvProtobufSerializer
import com.google.common.primitives.{Shorts, UnsignedBytes}

/**
 * Default protocol used by NRV. All nodes must have this protocol, since it's
 * used for cluster management.
 */
class NrvProtocol(localNode: LocalNode,
                  idleConnectionTimeoutMs: Long,
                  maxConnectionPoolSize: Int)
  extends Protocol("nrv") {

  override val transport = new NrvNettyTransport(localNode.listenAddress, localNode.ports(name),
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

  def parse(message: AnyRef): Message = {
    val bytes = message.asInstanceOf[Array[Byte]]

    val magicByte: Int = UnsignedBytes.toInt(bytes(0))

    if (magicByte == (UnsignedBytes.toInt(NrvProtocol.NrvMagicByte)))
      parseProtobuf(bytes)
    else
      throw new RuntimeException("The magic byte was not recognized.")
  }

  def generate(message: Message): AnyRef = {
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

    buffer.put(NrvProtocol.NrvMagicByte)

    buffer.put(bytes)

    buffer.array()
  }
}

object NrvProtocol {
  private val NrvMagicByte : Byte = (0xF2).toByte
}
