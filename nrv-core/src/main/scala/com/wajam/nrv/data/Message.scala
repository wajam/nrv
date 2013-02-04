package com.wajam.nrv.data

import com.wajam.nrv.cluster.Node
import scala.Option
import com.wajam.nrv.service.{Endpoints, ActionMethod, ActionURL}

/**
 * Base used for outbound and inbound messages.
 */
abstract class Message(params: Iterable[(String, Any)] = null,
                       meta: Iterable[(String, Any)] = null,
                       data: Any = null,
                       var code: Int = 200) extends Serializable {

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
  var destination: Endpoints = Endpoints.EMPTY
  var token: Long = -1

  val parameters = new collection.mutable.HashMap[String, Any]
  val metadata = new collection.mutable.HashMap[String, Any]

  // TODO: StringMigration: Rename (remove new suffix) when old "[String, Any]" are removed
  val parametersNew = new collection.mutable.HashMap[String, Seq[String]]
  val metadataNew = new collection.mutable.HashMap[String, Seq[String]]

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

    other.code = this.code

    other.protocolName = this.protocolName
    other.serviceName = this.serviceName
    other.method = this.method
    other.path = this.path
    other.rendezvousId = this.rendezvousId

    other.error = this.error

    other.function = this.function

    other.source = this.source
    other.destination = this.destination // TODO: should be cloned
    other.token = this.token

    other.parameters ++= this.parameters
    other.metadata ++= this.metadata
    other.messageData = this.messageData
  }

  override def toString = {
    new StringBuilder("message [")
      .append("protocol name=" + protocolName)
      .append(", service name=" + serviceName)
      .append(", function call=" + (function == FUNCTION_CALL).toString)
      .append(", path=" + path)
      .append(", method=" + method)
      .append(", parameters=" + parameters)
      .append(", code=" + code).append("]").toString()
  }
}

object MessageType {
  val FUNCTION_CALL = 0
  val FUNCTION_RESPONSE = 1
}

class SerializableMessage(params: Iterable[(String, Any)] = null,
                          meta: Iterable[(String, Any)] = null,
                          data: Any = null) extends Message(params, meta, data) with Serializable {

}

object SerializableMessage {
  def apply(message: Message) = {
    val serMessage = new SerializableMessage()
    message.copyBaseMessageData(serMessage)
    serMessage
  }
}
