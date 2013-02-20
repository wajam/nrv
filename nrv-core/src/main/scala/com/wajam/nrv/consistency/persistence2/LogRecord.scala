package com.wajam.nrv.consistency.persistence2

import com.wajam.nrv.utils.timestamp.Timestamp
import com.wajam.nrv.data.{MessageType, Message}
import java.io._
import com.wajam.nrv.protocol.codec.{MessageJavaSerializeCodec, Codec}
import com.wajam.nrv.consistency.persistence2.LogRecord.{Index, Response, Request}
import com.wajam.nrv.consistency.Consistency

sealed trait LogRecord {
  val id: Long
  val consistentTimestamp: Option[Timestamp]
  //  val timestamp: Timestamp
  //  val token: Long
}

object LogRecord {

  def apply(id: Long, consistentTimestamp: Option[Timestamp], message: Message): LogRecord = {
    message.function match {
      case MessageType.FUNCTION_CALL => Request(id, consistentTimestamp, message)
      case MessageType.FUNCTION_RESPONSE => Response(id, consistentTimestamp, message)
    }
  }

  case class Request(id: Long, consistentTimestamp: Option[Timestamp], timestamp: Timestamp, token: Long,
                     message: Message) extends LogRecord {
    override val hashCode: Int = {
      41 * (
        41 * (
          41 * (
            41 * (
              41 * (
                41 * (
                  41 + id.hashCode
                  ) + consistentTimestamp.hashCode
                ) + timestamp.hashCode
              ) + token.hashCode
            ) + message.parameters.hashCode
          ) + message.metadata.hashCode()
        ) + (if (message.messageData == null) 0 else message.messageData.hashCode())
    }

    override def equals(other: Any): Boolean =
      other match {
        case that: Request => {
          id == that.id && consistentTimestamp == that.consistentTimestamp && timestamp == that.timestamp &&
            token == that.token && message.parameters == that.message.parameters &&
            message.metadata == that.message.metadata && message.messageData == that.message.messageData
        }
        case _ => false
      }
  }

  object Request {
    def apply(id: Long, consistentTimestamp: Option[Timestamp], message: Message): Request = {
      require(message.function == MessageType.FUNCTION_CALL)
      Request(id, consistentTimestamp, Consistency.getMessageTimestamp(message).get, message.token, message)
    }
  }

  sealed trait Status {
    def code: Int
  }

  case class Response(id: Long, consistentTimestamp: Option[Timestamp], timestamp: Timestamp, token: Long,
                      status: Status) extends LogRecord {
    def isSuccess = status == Response.Success
  }

  object Response {
    def apply(id: Long, consistentTimestamp: Option[Timestamp], message: Message): Response = {
      require(message.function == MessageType.FUNCTION_RESPONSE)
      val status = if (message.code >= 200 && message.code < 300 && message.error.isEmpty) Success else Error
      Response(id, consistentTimestamp, Consistency.getMessageTimestamp(message).get, message.token, status)
    }

    object Success extends Status {
      val code = 1
    }

    object Error extends Status {
      val code = 0
    }
  }

  case class Index(id: Long, consistentTimestamp: Option[Timestamp]) extends LogRecord with Ordered[Index] {
    override def compare(that: Index) = compareTo(that)

    override def compareTo(that: Index): Int = {
      id.compareTo(that.id) match {
        case 0 => consistentTimestamp.getOrElse(Timestamp(Long.MinValue)).compareTo(
          that.consistentTimestamp.getOrElse(Timestamp(Long.MinValue)))
        case r => r // TODO: if (id > that.id) require(timestamp >= that.timestamp)
      }
    }
  }

  implicit def record2Index(record: LogRecord) = {
    record match {
      case index: Index => index
      case _ => Index(record.id, record.consistentTimestamp)
    }
  }
}

class LogRecordSerializer {

  import LogRecordSerializer._

  @throws(classOf[IOException])
  def serialize(record: LogRecord): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    val dos = new DataOutputStream(baos)

    record match {
      case req: Request => {
        dos.writeInt(RequestType)
        writeRequest(req, dos)
      }
      case res: Response => {
        dos.writeInt(ResponseType)
        writeResponse(res, dos)
      }
      case i: Index => {
        dos.writeInt(IndexType)
        writeIndex(i, dos)
      }
    }
    dos.flush()
    baos.toByteArray
  }

  @throws(classOf[IOException])
  private def writeRequest(record: Request, dos: DataOutputStream) {
    dos.writeLong(record.id)
    writeTimestampOption(record.consistentTimestamp, dos)
    dos.writeLong(record.timestamp.value)
    dos.writeLong(record.token)
    val encodedMessage = codec.encode(record.message)
    dos.writeInt(encodedMessage.length)
    dos.write(encodedMessage)
  }

  @throws(classOf[IOException])
  private def writeResponse(record: Response, dos: DataOutputStream) {
    dos.writeLong(record.id)
    writeTimestampOption(record.consistentTimestamp, dos)
    dos.writeLong(record.timestamp.value)
    dos.writeLong(record.token)
    dos.writeInt(record.status.code)
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
  def deserialize(data: Array[Byte]): LogRecord = {
    val bais = new ByteArrayInputStream(data)
    val dis = new DataInputStream(bais)

    dis.readInt() match {
      case RequestType => {
        readRequest(dis)
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
  private def readRequest(dis: DataInputStream): Request = {
    val id = dis.readLong()
    val consistentTimestamp = readTimestampOption(dis)
    val timestamp = Timestamp(dis.readLong())
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

    Request(id, consistentTimestamp, timestamp, token, message)
  }

  @throws(classOf[IOException])
  private def readResponse(dis: DataInputStream): Response = {
    val id = dis.readLong()
    val consistentTimestamp = readTimestampOption(dis)
    val timestamp = Timestamp(dis.readLong())
    val token = dis.readLong()
    val status = dis.readInt() match {
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

object LogRecordSerializer {
  val MinMessageLen = 0
  val MaxMessageLen = 1000000

  val RequestType = 1
  val ResponseType = 2
  val IndexType = 3

  // TODO: Change to protobuf codec when ready
  val codec: Codec = new MessageJavaSerializeCodec
}

