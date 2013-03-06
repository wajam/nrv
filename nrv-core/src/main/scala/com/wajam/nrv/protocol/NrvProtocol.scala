package com.wajam.nrv.protocol

import java.nio.ByteBuffer
import codec.{MessageJavaSerializeCodec, Codec}
import com.wajam.nrv.data.{OutMessage, Message}
import com.wajam.nrv.transport.nrv.NrvNettyTransport
import com.wajam.nrv.cluster.LocalNode
import com.wajam.nrv.data.serialization.NrvProtobufSerializer
import com.google.common.primitives.{Shorts, UnsignedBytes}
import com.wajam.nrv.service.Action

/**
 * Default protocol used by NRV. All nodes must have this protocol, since it's
 * used for cluster management.
 */
class NrvProtocol(localNode: LocalNode, protocolVersion: NrvProtocolVersion.Value = NrvProtocolVersion.V2)
  extends Protocol("nrv") {

  override val transport = new NrvNettyTransport(localNode.listenAddress, localNode.ports(name), this)

  val javaSerializer = new MessageJavaSerializeCodec()

  val registeredCodecs =  new collection.mutable.HashMap[String, Codec]

  def start() {
    transport.start()
    log.info("Using protocol {}", protocolVersion)
  }

  def stop() {
    transport.stop()
  }

  override def bindAction(action: Action) {

    // Register the codec to the list of codec
    val (name, codec) = action.dataBinaryCodec

    val oldCodec = registeredCodecs.get(name)

    oldCodec match {
      case Some(oldCodec) if oldCodec.getClass != codec.getClass =>
        throw new UnsupportedOperationException("Trying to register a different codec with the same name")
      case Some(oldCodec) =>
        // Noop already registered
      case None => registeredCodecs += (name -> codec)
    }

    // Resume binding
    super.bindAction(action)
  }

  override def handleOutgoing(action: Action, message: OutMessage) {

    // Set contentType, this will allow to right codec to be used to encode
    // on this side, but also will give hint to the receiver on how to decode it.
    val (name, _) = action.dataBinaryCodec
    message.contentType = Some(name)

    // Resume outgoing
    super.handleOutgoing(action, message)
  }

  def parse(message: AnyRef): Message = {
    val bytes = message.asInstanceOf[Array[Byte]]

    val magicShort: Int = Shorts.fromByteArray(bytes) & 0xFFFF
    val magicByte: Int = UnsignedBytes.toInt(bytes(0))

    if (magicShort == (NrvProtocol.JavaSerializeMagicShort & 0xFFFF))
      parseV1(bytes)
    else if (magicByte == (UnsignedBytes.toInt(NrvProtocol.V2MagicByte)))
      parseV2(bytes)
    else
      throw new RuntimeException("The magic byte was not recognized.")
  }

  def generate(message: Message): AnyRef = {
    if (protocolVersion == NrvProtocolVersion.V1)
      generateV1(message)
    else if (protocolVersion == NrvProtocolVersion.V2)
      generateV2(message)
    else
      throw new RuntimeException("The provided version number is invalid.")
  }

  private def parseV2(message: Array[Byte]): Message = {

    val protobufSerializer = new NrvProtobufSerializer(registeredCodecs.toMap)

    if (message.length < 1)
      throw new IllegalArgumentException("message needs at least one byte of data")

    val messageLength = message.length - 1

    val bytes = new Array[Byte](messageLength)

    // Get bytes without the magic byte
    val buffer = ByteBuffer.wrap(message)
    buffer.get()
    buffer.get(bytes)

    protobufSerializer.deserializeMessage(bytes)
  }

  private def generateV2(message: Message): Array[Byte] = {

    val protobufSerializer = new NrvProtobufSerializer(registeredCodecs.toMap)

    val bytes = protobufSerializer.serializeMessage(message)

    val messageLength = bytes.length + 1

    val buffer = ByteBuffer.allocate(messageLength)

    buffer.put(NrvProtocol.V2MagicByte)

    buffer.put(bytes)

    buffer.array()
  }

  private def parseV1(message: Array[Byte]): Message = {
    javaSerializer.decode(message).asInstanceOf[Message]
  }

  private def generateV1(message: Message): Array[Byte] = {
    javaSerializer.encode(message)
  }
}

object NrvProtocol {
  // Source: http://docs.oracle.com/javase/6/docs/platform/serialization/spec/protocol.html
  private val JavaSerializeMagicShort : Short = (0xACED).toShort

  private val V2MagicByte : Byte = (0xF2).toByte
}

object NrvProtocolVersion extends Enumeration {

  // Old serialized message
  val V1 = Value(1)

  // Protobuf serialization for Message, except for Any object in parameters, metadata
  // and errors which are Java serialized. Codec for message data.
  val V2 = Value(2)
}
