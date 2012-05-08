package com.wajam.nrv.protocol.codec

import java.io._
import com.wajam.nrv.data.{SerializableMessage, Message}

/**
 * Codec that uses Java object serialization to encode messages
 */
class JavaSerializeCodec extends Codec {
  def encodeAny(obj: AnyRef): Array[Byte] = {
    val baos = new ByteArrayOutputStream();
    val serializer = new ObjectOutputStream(baos)
    serializer.writeObject(obj)
    serializer.flush()
    baos.toByteArray
  }

  override def encode(message: Any, context: Any = null): Array[Byte] = {
    // create a new message that won't have In/Out message specific fields
    val serMessage = SerializableMessage(message.asInstanceOf[Message])

    this.encodeAny(serMessage)
  }

  override def decode(data: Array[Byte], context: Any = null): Any = {
    val bains = new ByteArrayInputStream(data)
    val deserialize = new ClassLoaderObjectInputStream(getClass.getClassLoader, bains)
    deserialize.readObject().asInstanceOf[Message]
  }

  def decodeAny(data: Array[Byte]): AnyRef = {
    val bains = new ByteArrayInputStream(data)
    val deserialize = new ClassLoaderObjectInputStream(getClass.getClassLoader, bains)
    deserialize.readObject()
  }
}

/**
 * Fixes serialization issues when run through sbt
 * @see https://github.com/harrah/xsbt/issues/163
 */
case class ClassLoaderObjectInputStream(classLoader: ClassLoader, inputStream: InputStream) extends ObjectInputStream(inputStream) {
  override def resolveClass(objectStreamClass: ObjectStreamClass): Class[_] = {
    val clazz = Class.forName(objectStreamClass.getName, false, classLoader)
    if (clazz != null) clazz
    else super.resolveClass(objectStreamClass)
  }
}
