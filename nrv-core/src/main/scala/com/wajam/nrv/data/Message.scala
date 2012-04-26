package com.wajam.nrv.data

import com.wajam.nrv.cluster.Node
import scala.Option
import com.wajam.nrv.service.{ActionMethod, ActionURL, Endpoints}

/**
 * Base used for outbound and inbound messages.
 */
abstract class Message(params: Iterable[(String, Any)] = null,
                       meta: Iterable[(String, Any)] = null,
                       data: Any = null) extends Serializable {

  import MessageType._

  var protocolName = ""
  var serviceName = ""
  var method = ActionMethod.ANY
  var path = "/"
  var rendezvousId = 0
  var error: Option[Exception] = None

  /*
   * Messages that are passed between nodes are not just RPC calls, but can also
   * be response or any control message.
   */
  var function = FUNCTION_CALL

  var source: Node = null
  var destination = Endpoints.empty // TODO: see @Action, should it be service members?

  val parameters = new collection.mutable.HashMap[String, Any]
  val metadata = new collection.mutable.HashMap[String, Any]
  var messageData: Any = null

  val attachments = new collection.mutable.HashMap[String, Any]

  loadData(params, meta, data)

  def this() = this(null, null, null)

  private def loadData(params: Iterable[(String, Any)] = null,
                        meta: Iterable[(String, Any)] = null,
                        data: Any = null) {
    if (params != null) {
      parameters ++= params
    }
    if (meta != null) {
      metadata ++= meta
    }
    messageData = data
  }

  lazy val actionURL = new ActionURL(serviceName, path, protocolName)

  def copyTo(other: Message) {
    copyBaseMessageData(other)
    other.attachments ++= attachments
  }

  def copyBaseMessageData(other: Message) {

    other.parameters ++= this.parameters
    other.metadata ++= this.metadata
    other.messageData = this.messageData
    other.protocolName = this.protocolName
    other.serviceName = this.serviceName
    other.method = this.method
    other.path = this.path
    other.rendezvousId = this.rendezvousId
    other.function = this.function
    other.error = this.error
    other.source = this.source
    other.method = this.method
    other.destination = this.destination // TODO: should be cloned
  }
}

object MessageType {
  val FUNCTION_CALL = 0
  val FUNCTION_RESPONSE = 1
}

class SerializableMessage(params: Iterable[(String, Any)] = null,
                          meta: Iterable[(String, Any)] = null,
                          data: Any = null) extends Message(params, meta, data) with Serializable{

}

object SerializableMessage {
  def apply(message: Message) = {
    val serMessage = new SerializableMessage()
    message.copyBaseMessageData(serMessage)
    serMessage
  }
}
