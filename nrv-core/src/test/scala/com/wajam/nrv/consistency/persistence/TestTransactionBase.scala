package com.wajam.nrv.consistency.persistence

import org.scalatest.FunSuite
import com.wajam.nrv.data.{InMessage, Message}
import com.wajam.nrv.utils.timestamp.Timestamp

abstract class TestTransactionBase extends FunSuite {
  def createTransaction(timestamp: Long, previousTimestamp: Long = -1, token: Long = 0, msg: Message = null) = {
    val previous = if (previousTimestamp == -1) None else Some(Timestamp(previousTimestamp))
    val message = if (msg != null) {
      msg
    } else {
      new InMessage((Map(("ts" -> timestamp), ("pt" -> previousTimestamp), ("tk" -> token))))
    }
    TransactionEvent(Timestamp(timestamp), previous, token, message)
  }

  def assertEquals(expected: TransactionEvent, actual: TransactionEvent) {
    expect(expected.timestamp, "timestamp") {
      actual.timestamp
    }
    expect(expected.previous, "previous") {
      actual.previous
    }
    expect(expected.token, "token") {
      actual.token
    }
    expect(expected.message.parameters, "params") {
      actual.message.parameters
    }
    expect(expected.message.metadata, "metadata") {
      actual.message.metadata
    }
    expect(expected.message.messageData, "data") {
      actual.message.messageData
    }
  }
}
