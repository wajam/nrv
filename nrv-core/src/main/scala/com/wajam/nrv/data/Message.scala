package com.wajam.nrv.data

import scala.collection.mutable.HashMap
import com.wajam.nrv.service.{ActionURL, Endpoints}
import com.wajam.nrv.cluster.Node

/**
 * Base used for outbound and inbound messages.
 */
abstract class Message(data: Iterable[(String, Any)]) extends HashMap[String, Any] with Serializable {

  import MessageType._

  var protocolName = ""
  var serviceName = ""
  var method = ""
  var path = "/"
  var rendezvous = 0

  var error: Option[Exception] = None

  /*
   * Messages that are passed between nodes are not just RPC calls, but can also
   * be response or any control message.
   */
  var function = FUNCTION_CALL

  var source: Node = null
  var destination = Endpoints.empty // TODO: see @Action, should it be service members??

  loadData(data)

  def this(params: (String, Any)*) = this(params)

  def loadData(data: Iterable[(String, Any)]) {
    this ++= data
  }

  lazy val actionURL = new ActionURL(serviceName, path, protocolName)

  def copyTo(other: Message) {
    other.loadData(this)
    other.protocolName = this.protocolName
    other.serviceName = this.serviceName
    other.method = this.method
    other.path = this.path
    other.rendezvous = this.rendezvous
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

class SerializableMessage extends Message {
}
