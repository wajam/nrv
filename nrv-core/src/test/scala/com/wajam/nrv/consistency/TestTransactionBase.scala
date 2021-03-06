package com.wajam.nrv.consistency

import org.scalatest.FunSuite
import com.wajam.nrv.data.{OutMessage, MessageType, InMessage, Message}

abstract class TestTransactionBase extends FunSuite {

  def createRequestMessage(timestamp: Long, token: Long = 0, data: Any = null): InMessage = {
    val request = new InMessage(Map("ts" -> timestamp, "tk" -> token), data = data)
    request.function = MessageType.FUNCTION_CALL
    request.token = token
    request.timestamp = Some(timestamp)
    request
  }

  def createResponseMessage(request: Message, code: Int = 200, error: Option[Exception] = None) = {
    val response = new OutMessage()
    request.copyTo(response)
    response.function = MessageType.FUNCTION_RESPONSE
    response.code = code
    response.error = error
    response
  }
}
