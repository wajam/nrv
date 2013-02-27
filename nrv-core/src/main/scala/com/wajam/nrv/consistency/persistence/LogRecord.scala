package com.wajam.nrv.consistency.persistence

import com.wajam.nrv.utils.timestamp.Timestamp
import com.wajam.nrv.data.{MessageType, Message}
import com.wajam.nrv.consistency.Consistency
import com.wajam.nrv.consistency.persistence.LogRecord.Response.Status

sealed trait LogRecord {
  val id: Long
  val consistentTimestamp: Option[Timestamp]
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
          ) + message.metadata.hashCode
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

    sealed trait Status {
      def code: Int

      override def toString = "status=" + code
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
        case r => r
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