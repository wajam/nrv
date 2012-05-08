package com.wajam.nrv.protocol.codec

/**
 * Message encoder/decoder used by a protocol
 */
trait Codec {
  def encode(entity: Any, context: Any = null): Array[Byte]

  def decode(data: Array[Byte], context: Any = null): Any
}
