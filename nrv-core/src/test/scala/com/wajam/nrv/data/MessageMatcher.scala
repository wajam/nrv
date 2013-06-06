package com.wajam.nrv.data

import org.mockito.ArgumentMatcher
import com.wajam.nrv.utils.Future

class MessageMatcher(params: Iterable[(String, MValue)],
                     metadata: Iterable[(String, MValue)], data: Any) extends ArgumentMatcher {

  private var matchedMessages: List[Message] = Nil
  private var replyableMessages: List[OutMessage] = Nil

  def capturedMessages = matchedMessages

  def matches(argument: Any) = {
    val message = argument.asInstanceOf[Message]

    // verify timestamp
    // verify token

    require(params.toMap == message.parameters.toMap,
      "params %s not equals to %s".format(message.parameters, params.toMap))

    require(metadata.toMap == message.metadata.toMap,
      "metadata %s not equals to %s".format(message.metadata, metadata.toMap))

    require(data == message.messageData, "data %s not equals to %s".format(message.messageData, data))

    matchedMessages = message :: matchedMessages
    message match {
      case outMessage: OutMessage => replyableMessages = outMessage :: replyableMessages
      case _ =>
    }
    true
  }

  def replyWith(message: InMessage) {
    replyWith(message, 0L)
  }

  def replyWith(reply: InMessage, delay: Long) {
    replyableMessages match {
      case outMessage :: remainingMessages => {
        replyableMessages = remainingMessages
        Future {
          // Cheap way to perform something in the future after a given delay but this is probably not playing
          // nice with thread pool.
          Thread.sleep(delay)
          outMessage.handleReply(reply)
        }
      }
      case _ => throw new IllegalStateException("No replyable message")
    }
  }

  def replyWith(exception: Exception) {
    replyWith(exception, 0L)
  }

  def replyWith(exception: Exception, delay: Long) {
    val message = new InMessage()
    message.error = Some(exception)
    replyWith(message, delay)
  }

  def replyWith(params: Iterable[(String, MValue)] = Iterable(),
                metadata: Iterable[(String, MValue)] = Iterable(),
                data: Any = null, delay: Long = 0L) {
    replyWith(new InMessage(params, metadata, data), delay)
  }
}

object MessageMatcher {
  def apply(params: Iterable[(String, MValue)] = Iterable(),
            metadata: Iterable[(String, MValue)] = Iterable(),
            data: Any = null) = new MessageMatcher(params, metadata, data)

  def matchMessage(params: Iterable[(String, MValue)] = Iterable(),
                   metadata: Iterable[(String, MValue)] = Iterable(),
                   data: Any = null) = MessageMatcher(params, metadata, data)
}

