package com.wajam.nrv.data

import scala.collection.mutable.HashMap
import com.wajam.nrv.service.{ActionURL, Endpoints}
import com.wajam.nrv.cluster.Node

/**
 * Base message used for outbound and inbound requests.
 */
abstract class Message(data: Iterable[(String, Any)]) extends HashMap[String, Any] with Serializable {

  import Message._

  var protocolName = ""
  var serviceName = ""
  var path = "/"
  var rendezvous = 0

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
    other.path = this.path
    other.function = this.function
    other.rendezvous = this.rendezvous
    other.source = this.source
    other.destination = this.destination // TODO: should be cloned
  }
}

object Message {
  val FUNCTION_CALL = 0
  val FUNCTION_RESPONSE = 1
}

class SerializableMessage  extends Message {
}
