package com.wajam.nrv.consistency.persistence

import com.wajam.nrv.data.Message
import java.io._
import com.wajam.nrv.utils.timestamp.Timestamp
import com.wajam.nrv.protocol.codec.{MessageJavaSerializeCodec, Codec}

case class TransactionEvent(timestamp: Timestamp, previous: Option[Timestamp], token: Long, message: Message)

class TransactionEventSerializer {
  import TransactionEventSerializer._

  @throws(classOf[IOException])
  def serialize(tx: TransactionEvent): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    val dos = new DataOutputStream(baos)

    dos.writeLong(tx.timestamp.value)
    dos.writeLong(tx.previous.getOrElse(Timestamp(-1)).value)
    dos.writeLong(tx.token)
    val encodedMessage = codec.encode(tx.message)
    dos.writeInt(encodedMessage.length)
    dos.write(encodedMessage)
    dos.flush()
    baos.toByteArray
  }

  @throws(classOf[IOException])
  def deserialize(data: Array[Byte]): TransactionEvent = {
    val bais = new ByteArrayInputStream(data)
    val dis = new DataInputStream(bais)

    val timestamp = Timestamp(dis.readLong())
    val previous = dis.readLong() match {
      case -1 => None
      case value => Some(Timestamp(value))
    }
    val token = dis.readLong()

    // Read message
    val messageLen = dis.readInt()
    if (messageLen < MinMessageLen || messageLen > MaxMessageLen) {
      throw new IOException("Message length %d is out of bound".format(messageLen))
    }
    val encodedMessage = new Array[Byte](messageLen)
    val readLen = dis.read(encodedMessage)
    if (readLen != messageLen) {
      throw new IOException("Read message length %d not equals to %d".format(readLen, messageLen))
    }
    val message = codec.decode(encodedMessage).asInstanceOf[Message]

    TransactionEvent(timestamp, previous, token, message)
  }
}

object TransactionEventSerializer {
  val MinMessageLen = 0
  val MaxMessageLen = 1000000

  // TODO: Change to protobuf codec when ready
  val codec: Codec = new MessageJavaSerializeCodec
}
