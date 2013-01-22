package com.wajam.nrv.protocol

import java.nio.ByteBuffer
import codec.{MessageJavaSerializeCodec, Codec}
import com.wajam.nrv.data.Message
import com.wajam.nrv.transport.nrv.NrvNettyTransport
import com.wajam.nrv.cluster.LocalNode
import com.wajam.nrv.UnsupportedProtocolException
import com.wajam.nrv.data.serialization.NrvProtobufSerializer

/**
 * Default protocol used by NRV. All nodes must have this protocol, since it's
 * used for cluster management.
 */
class NrvProtocol(localNode: LocalNode, codec: Codec = new MessageJavaSerializeCodec, protocolVersion: NrvProtocolVersion.Value = NrvProtocolVersion.V1)
  extends Protocol("nrv") {

  override val transport = new NrvNettyTransport(localNode.listenAddress, localNode.ports(name), this)

  val protobufSerializer = new NrvProtobufSerializer()

  def start() {
    transport.start()
  }

  def stop() {
    transport.stop()
  }

  def parse(message: AnyRef): Message = {
    val bytes = message.asInstanceOf[Array[Byte]]

    val magicShort = ByteBuffer.wrap(bytes, 0, 2).getInt()
    val magicByte = bytes(0)

    if (magicShort == NrvProtocol.JavaSerializeMagicByte)
      parseV1(bytes)
    else if (magicByte == NrvProtocol.V2MagicByte)
      parseV2(bytes)
    else
      throw new UnsupportedProtocolException("The magic byte was not recognized.")
  }

  def generate(message: Message): AnyRef = {
    if (protocolVersion == NrvProtocolVersion.V1)
      generateV1(message)
    else if (protocolVersion == NrvProtocolVersion.V2)
      generateV2(message)
    else
      throw new UnsupportedProtocolException("The provided version number is invalid.")
  }

  private def parseV2(message: Array[Byte]): Message = {

    val messageLength = message.length - 1

    val bytes = new Array[Byte](messageLength)

    // Get bytes without the magic byte
    ByteBuffer.wrap(message).get(bytes, 1, messageLength)

    protobufSerializer.deserializeMessage(bytes)
  }

  private def generateV2(message: Message): Array[Byte] = {

    val bytes = protobufSerializer.serializeMessage(message)

    val messageLength = bytes.length + 1

    val buffer = ByteBuffer.allocate(messageLength)

    buffer.put(NrvProtocol.V2MagicByte)

    buffer.put(bytes)

    buffer.array()
  }

  private def parseV1(message: Array[Byte]): Message = {
    codec.decode(message).asInstanceOf[Message]
  }

  private def generateV1(message: Message): Array[Byte] = {
    codec.encode(message)
  }
}

object NrvProtocol {
  // Source: http://docs.oracle.com/javase/6/docs/platform/serialization/spec/protocol.html
  private val JavaSerializeMagicByte : Short = (0xACED).toShort

  private val V2MagicByte : Byte = (0xF2).toByte
}

object NrvProtocolVersion extends Enumeration {

  // Old serialized message
  val V1 = Value(1)

  // Protobuf serialization for Message, except for Any object in parameters, metadata
  // and errors which are Java serialized. Codec for message data.
  val V2 = Value(2)
}
