package com.wajam.nrv.protocol.codec

import java.io._
import scala.annotation.tailrec

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
 * Hybrid codec that can handle both streams and in-memory data.
 * First buffers data if needed, then delegates to an underlying codec.
 * This is the default NRV codec.
 */
class HybridCodec(underlyingCodec: Codec = new GenericJavaSerializeCodec) extends Codec {

  def loadStream(is: InputStream): Array[Byte] = {
    val os = new ByteArrayOutputStream()

    val bufferSize = 16384
    val buffer = new Array[Byte](bufferSize)

    @tailrec
    def copyToOS(is: InputStream, os: ByteArrayOutputStream, buffer: Array[Byte]) {
      val bytesRead = is.read(buffer, 0, bufferSize)
      if(bytesRead != -1) {
        os.write(buffer, 0, bytesRead)
        copyToOS(is, os, buffer)
      }
    }

    copyToOS(is, os, buffer)

    os.flush()

    os.toByteArray
  }

  override def encode(obj: Any, context: Any = null): Array[Byte] = {
    val serializable = obj match {
      case is: InputStream =>
        loadStream(is)
      case other =>
        other
    }
    underlyingCodec.encode(serializable, context)
  }

  override def decode(data: Array[Byte], context: Any = null): Any = {
    underlyingCodec.decode(data, context)
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
