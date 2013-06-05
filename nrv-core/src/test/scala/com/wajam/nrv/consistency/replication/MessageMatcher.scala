package com.wajam.nrv.consistency.replication

import com.wajam.nrv.data.{OutMessage, Message, InMessage, MValue}
import org.mockito.ArgumentMatcher
import com.wajam.nrv.utils.Future

class MessageMatcher(params: Iterable[(String, MValue)],
                     metadata: Iterable[(String, MValue)], data: Any) extends ArgumentMatcher {

  private var replyMessages: List[(InMessage, Long)] = Nil

  def matches(argument: Any) = {
    val message = argument.asInstanceOf[Message]

    // verify timestamp
    // verify token

    require(params.toMap == message.parameters.toMap,
      "params %s not equals to %s".format(message.parameters, params.toMap))

    require(metadata.toMap == message.metadata.toMap,
      "metadata %s not equals to %s".format(message.metadata, metadata.toMap))

    require(data == message.messageData, "data %s not equals to %s".format(message.messageData, data))

    (replyMessages, message) match {
      case ((reply, delay) :: remainingReplies, out: OutMessage) => {
        println("MessageMatcher.matches %s".format(message))
        replyMessages = remainingReplies
        Future {
          // Cheap way to perform something in the future after a given delay but this is probably not playing
          // nice with thread pool.
          Thread.sleep(delay)
          out.handleReply(reply)
        }
      }
      case _ =>
    }

    true
  }

  def replyWith(message: InMessage): MessageMatcher = {
    replyWith(message, 0L)
  }

  def replyWith(message: InMessage, delay: Long): MessageMatcher = {
    replyMessages = (message, delay) :: replyMessages
    this
  }

  def replyWith(exception: Exception): MessageMatcher = {
    replyWith(exception, 0L)
  }

  def replyWith(exception: Exception, delay: Long): MessageMatcher = {
    val message = new InMessage()
    message.error = Some(exception)
    replyWith(message, delay)
  }

  def replyWith(params: Iterable[(String, MValue)] = Iterable(),
                  metadata: Iterable[(String, MValue)] = Iterable(),
                  data: Any = null, delay: Long = 0L): MessageMatcher = {
    replyWith(new InMessage(params, metadata, data), delay)
  }
}

object MessageMatcher {
  def matchMessage(params: Iterable[(String, MValue)] = Iterable(),
                   metadata: Iterable[(String, MValue)] = Iterable(),
                   data: Any = null) = new MessageMatcher(params, metadata, data)
}

