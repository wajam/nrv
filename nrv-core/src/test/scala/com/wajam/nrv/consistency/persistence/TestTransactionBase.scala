package com.wajam.nrv.consistency.persistence

import org.scalatest.FunSuite
import com.wajam.nrv.data.{OutMessage, MessageType, InMessage, Message}
import com.wajam.nrv.utils.timestamp.Timestamp
import com.wajam.nrv.consistency.Consistency

abstract class TestTransactionBase extends FunSuite {

  def createRequestMessage(timestamp: Long, token: Long = 0): InMessage = {
    val request = new InMessage((Map(("ts" -> timestamp), ("tk" -> token))))
    request.function = MessageType.FUNCTION_CALL
    request.token = token
    Consistency.setMessageTimestamp(request, Timestamp(timestamp))
    request
  }

  def createResponseMessage(request: InMessage, code: Int = 200, error: Option[Exception] = None) = {
    val response = new OutMessage()
    request.copyTo(response)
    response.function = MessageType.FUNCTION_RESPONSE
    response.code = code
    response.error = error
    response
  }
}
