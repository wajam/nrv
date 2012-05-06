package com.wajam.nrv.protocol.codec

import com.wajam.nrv.data.Message


/**
 * Message encoder/decoder used by a protocol
 */
trait Codec {
  def encode(message: Message): Array[Byte]

  def decode(data: Array[Byte]): Message
}
