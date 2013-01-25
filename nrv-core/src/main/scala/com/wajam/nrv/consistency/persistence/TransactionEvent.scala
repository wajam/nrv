package com.wajam.nrv.consistency.persistence

import com.wajam.nrv.data.Message
import java.io._
import com.wajam.nrv.utils.timestamp.Timestamp
import com.wajam.nrv.protocol.codec.{MessageJavaSerializeCodec, Codec}
import java.util.zip.{CheckedInputStream, CRC32, CheckedOutputStream}

case class TransactionEvent(timestamp: Timestamp, previous: Option[Timestamp], token: Long, message: Message) {
  def writeToBytes(): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    val dos = new DataOutputStream(baos)

    dos.writeLong(timestamp.value)
    dos.writeLong(previous.getOrElse(Timestamp(-1)).value)
    dos.writeLong(token)
    val encodedMessage = TransactionEvent.codec.encode(message)
    dos.writeInt(encodedMessage.length)
    dos.write(encodedMessage)
    dos.flush()
    baos.toByteArray
  }
}

object TransactionEvent {
  val MinMessageLen = 0
  val MaxMessageLen = 1000000

  val codec: Codec = new MessageJavaSerializeCodec

  def apply(data: Array[Byte]): TransactionEvent = {
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
    val message = TransactionEvent.codec.decode(encodedMessage).asInstanceOf[Message]

    TransactionEvent(timestamp, previous, token, message)
  }
}
