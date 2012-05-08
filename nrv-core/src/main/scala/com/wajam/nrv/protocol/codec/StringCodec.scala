package com.wajam.nrv.protocol.codec

/**
 *
 */

class StringCodec extends Codec {

  def encode(entity: Any, context: Any) = {
    entity.asInstanceOf[String].getBytes(context.asInstanceOf[String])
  }

  def decode(data: Array[Byte], context: Any) = {
    new String(data, context.asInstanceOf[String])
  }
}
