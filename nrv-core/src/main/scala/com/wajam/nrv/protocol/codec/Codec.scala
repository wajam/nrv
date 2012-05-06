package com.wajam.nrv.protocol.codec

/**
 * Message encoder/decoder used by a protocol
 */
trait Codec[ObjectifiedType] {
  def encode(entity: ObjectifiedType): Array[Byte]

  def decode(data: Array[Byte]): ObjectifiedType
}
