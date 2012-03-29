package com.wajam.nrv.codec

import com.wajam.nrv.data.Message


/**
 * Message encoder/decoder used by a protocol
 */
abstract class Codec {
  def encode(message: Message): Array[Byte]

  def decode(data: Array[Byte]): Message
}
