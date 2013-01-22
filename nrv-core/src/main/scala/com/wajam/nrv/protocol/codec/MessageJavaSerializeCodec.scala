package com.wajam.nrv.protocol.codec

import java.io._
import com.wajam.nrv.data.{SerializableMessage, Message}

/**
 * Codec that uses Java object serialization to encode messages
 */
class MessageJavaSerializeCodec extends Codec {

  private val serializeCodec = new GenericJavaSerializeCodec()

  override def encode(message: Any, context: Any = null): Array[Byte] = {
    // create a new message that won't have In/Out message specific fields
    val serMessage = SerializableMessage(message.asInstanceOf[Message])

    serializeCodec.encodeAny(serMessage)
  }

  override def decode(data: Array[Byte], context: Any = null): Any = {
    val bains = new ByteArrayInputStream(data)
    val deserialize = new ClassLoaderObjectInputStream(getClass.getClassLoader, bains)
    deserialize.readObject().asInstanceOf[Message]
  }
}
