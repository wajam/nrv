package com.wajam.nrv.consistency.log

import com.wajam.nrv.protocol.codec.Codec
import com.wajam.nrv.data.Message
import com.wajam.nrv.data.serialization.NrvProtobufSerializer

/**
 * Encode/decode NRV message using protobuf. The message params and metadata use the built in NRV protobuf format.
 * The message data is encoded/decoded by the specified codec.
 */
class MessageProtobufCodec(dataCodec: Codec) extends Codec {
  def encode(entity: Any, context: Any) = {
    entity match {
      case message: Message => NrvProtobufSerializer.serializeMessage(message, (_) => dataCodec)
      case null => new Array[Byte](0)
      case _ => throw new IllegalArgumentException("Unsupported type: " + entity)
    }
  }

  def decode(data: Array[Byte], context: Any) = {
    if (data.length > 0) {
      NrvProtobufSerializer.deserializeMessage(data, (_) => dataCodec)
    } else {
      null
    }
  }
}
