package com.wajam.nrv.protocol.codec

import java.io._

class GenericJavaSerializeCodec extends Codec {

  def encodeAny(obj: AnyRef): Array[Byte] = {
    val baos = new ByteArrayOutputStream();
    val serializer = new ObjectOutputStream(baos)
    serializer.writeObject(obj)
    serializer.flush()
    baos.toByteArray
  }

  def decodeAny(data: Array[Byte]): AnyRef = {
    val bains = new ByteArrayInputStream(data)
    val deserialize = new ClassLoaderObjectInputStream(getClass.getClassLoader, bains)
    deserialize.readObject()
  }

  override def encode(obj: Any, context: Any = null): Array[Byte] = {
    this.encodeAny(obj.asInstanceOf[AnyRef])
  }

  override def decode(data: Array[Byte], context: Any = null): Any = {
    this.decodeAny(data)
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
