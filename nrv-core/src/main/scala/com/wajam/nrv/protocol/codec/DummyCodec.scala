package com.wajam.nrv.protocol.codec

/**
 * Dummy codec that can used in test, or as default
 *
 */
class DummyCodec() extends Codec {
  var hasEncoded = false
  var hasDecoded = false

  def encode(entity: Any, context: Any = null): Array[Byte] = {
    hasEncoded = true
    new Array[Byte](0)
  }

  def decode(data: Array[Byte], context: Any = null): Any = {
    hasDecoded = true
    null
  }
}
