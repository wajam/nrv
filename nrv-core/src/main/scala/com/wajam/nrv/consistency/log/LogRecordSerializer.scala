package com.wajam.nrv.consistency.log

import java.io._
import com.wajam.nrv.data.Message
import com.wajam.nrv.consistency.log.LogRecord.{Index, Response, Request}
import com.wajam.nrv.protocol.codec.{GenericJavaSerializeCodec, Codec}
import LogRecordSerializer._
import com.wajam.nrv.utils.timestamp.Timestamp

class LogRecordSerializer(dataCodec: Codec = DefaultDataCodec) {

  private[consistency] val messageCodec = new MessageProtobufCodec(dataCodec)

  @throws(classOf[IOException])
  def serialize(record: LogRecord, maxMessageLen: Int = DefaultMaxMessageLen): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    val dos = new DataOutputStream(baos)

    record match {
      case req: Request => {
        dos.writeShort(RequestType)
        writeRequest(req, dos, maxMessageLen)
      }
      case res: Response => {
        dos.writeShort(ResponseType)
        writeResponse(res, dos)
      }
      case i: Index => {
        dos.writeShort(IndexType)
        writeIndex(i, dos)
      }
    }
    dos.flush()
    baos.toByteArray
  }

  @throws(classOf[IOException])
  private def writeRequest(record: Request, dos: DataOutputStream, maxMessageLen: Int) {
    dos.writeLong(record.id)
    writeTimestampOption(record.consistentTimestamp, dos)
    dos.writeLong(record.timestamp.value)
    dos.writeLong(record.token)
    val encodedMessage = messageCodec.encode(record.message)
    validateMessageLength(encodedMessage.length, maxMessageLen)
    dos.writeInt(encodedMessage.length)
    dos.write(encodedMessage)
  }

  private def validateMessageLength(messageLen: Int, maxMessageLen: Int) {
    require(messageLen >= 0 && messageLen <= maxMessageLen,
      s"Message length $messageLen is out of bound")
  }

  @throws(classOf[IOException])
  private def writeResponse(record: Response, dos: DataOutputStream) {
    dos.writeLong(record.id)
    writeTimestampOption(record.consistentTimestamp, dos)
    dos.writeLong(record.timestamp.value)
    dos.writeLong(record.token)
    dos.writeShort(record.status.code)
  }

  @throws(classOf[IOException])
  private def writeIndex(record: Index, dos: DataOutputStream) {
    dos.writeLong(record.id)
    writeTimestampOption(record.consistentTimestamp, dos)
  }

  @throws(classOf[IOException])
  private def writeTimestampOption(timestamp: Option[Timestamp], dos: DataOutputStream) {
    dos.writeLong(timestamp.getOrElse(Timestamp(-1)).value)
  }

  @throws(classOf[IOException])
  def deserialize(data: Array[Byte], maxMessageLen: Int = DefaultMaxMessageLen): LogRecord = {
    val bais = new ByteArrayInputStream(data)
    val dis = new DataInputStream(bais)

    dis.readShort() match {
      case RequestType => {
        readRequest(dis, maxMessageLen)
      }
      case ResponseType => {
        readResponse(dis)
      }
      case IndexType => {
        readIndex(dis)
      }
    }
  }

  @throws(classOf[IOException])
  private def readRequest(dis: DataInputStream, maxMessageLen: Int): Request = {
    val id = dis.readLong()
    val consistentTimestamp = readTimestampOption(dis)
    val timestamp = Timestamp(dis.readLong())
    val token = dis.readLong()

    // Read message
    val messageLen = dis.readInt()
    validateMessageLength(messageLen, maxMessageLen)
    val encodedMessage = new Array[Byte](messageLen)
    dis.readFully(encodedMessage)
    lazy val message = messageCodec.decode(encodedMessage).asInstanceOf[Message]

    Request(id, consistentTimestamp, timestamp, token, new MessageProxy {
      def getMessage = message
    })
  }

  @throws(classOf[IOException])
  private def readResponse(dis: DataInputStream): Response = {
    val id = dis.readLong()
    val consistentTimestamp = readTimestampOption(dis)
    val timestamp = Timestamp(dis.readLong())
    val token = dis.readLong()
    val status = dis.readShort() match {
      case Response.Success.code => Response.Success
      case Response.Error.code => Response.Error
    }

    Response(id, consistentTimestamp, timestamp, token, status)
  }

  @throws(classOf[IOException])
  private def readIndex(dis: DataInputStream): Index = {
    val id = dis.readLong()
    val consistentTimestamp = readTimestampOption(dis)
    Index(id, consistentTimestamp)
  }

  @throws(classOf[IOException])
  private def readTimestampOption(dis: DataInputStream): Option[Timestamp] = {
    dis.readLong() match {
      case -1 => None
      case value => Some(Timestamp(value))
    }
  }
}

private[log] object LogRecordSerializer {
  val DefaultMaxMessageLen = 1000000

  val RequestType = 1
  val ResponseType = 2
  val IndexType = 3

  val DefaultDataCodec: Codec = new GenericJavaSerializeCodec
}
