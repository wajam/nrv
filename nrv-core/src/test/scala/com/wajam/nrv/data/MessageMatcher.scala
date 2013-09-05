package com.wajam.nrv.data

import org.mockito.ArgumentMatcher
import org.hamcrest.Description

class MessageMatcher[T <: Message](params: Iterable[(String, MValue)],
                     metadata: Iterable[(String, MValue)], data: Any) extends ArgumentMatcher {

  private var matchedMessages: List[T] = Nil

  def capturedMessages = matchedMessages
  def capturedMessage = matchedMessages.head

  def matches(argument: Any) = {
    val message = argument.asInstanceOf[T]

    // verify path
    // verify function call
    // verify timestamp
    // verify token
    val result = params.toMap == message.parameters.toMap &&
      metadata.toMap == message.metadata.toMap &&
      data == message.messageData

    matchedMessages = message :: matchedMessages

    result
  }

  override def describeTo(description: Description) {
    description.appendValue("message [parameters=%s, metadata=%s, data=%s]".format(params, metadata, data))
  }

  def replyCapturedMessageWith(reply: InMessage) {
    capturedMessage match {
      case outMessage: OutMessage => outMessage.handleReply(reply)
      case _ => throw new IllegalStateException("No replyable message")
    }
  }

  def replyCapturedMessageWith(exception: Exception) {
    val message = new InMessage()
    message.error = Some(exception)
    replyCapturedMessageWith(message)
  }

  def replyCapturedMessageWith(params: Iterable[(String, MValue)] = Iterable(),
                metadata: Iterable[(String, MValue)] = Iterable(),
                data: Any = null) {
    replyCapturedMessageWith(new InMessage(params, metadata, data))
  }
}

object MessageMatcher {
  def apply[T <: Message](params: Iterable[(String, MValue)] = Iterable(),
            metadata: Iterable[(String, MValue)] = Iterable(),
            data: Any = null) = new MessageMatcher[T](params, metadata, data)

  def matchMessage[T <: Message](params: Iterable[(String, MValue)] = Iterable(),
                   metadata: Iterable[(String, MValue)] = Iterable(),
                   data: Any = null) = MessageMatcher[T](params, metadata, data)

  def matchMessage[T <: Message](message: T) = MessageMatcher[T](message.parameters, message.metadata, message.messageData)
}

