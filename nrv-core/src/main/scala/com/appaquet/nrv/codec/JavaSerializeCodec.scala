package com.appaquet.nrv.codec

import com.appaquet.nrv.data.Message
import java.io.{ObjectInputStream, ByteArrayInputStream, ByteArrayOutputStream, ObjectOutputStream}


/**
 * Codec that uses Java object serialization to encode messages
 */
class JavaSerializeCodec extends Codec {
  def encode(message: Message): Array[Byte] = {
    val baos = new ByteArrayOutputStream();
    val serializer = new ObjectOutputStream(baos)
    serializer.writeObject(message)

    baos.toByteArray
  }

  def decode(data: Array[Byte]): Message = {
    val bains = new ByteArrayInputStream(data)
    val deserialize = new ObjectInputStream(bains)
    deserialize.readObject().asInstanceOf[Message]
  }
}
