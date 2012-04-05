package com.wajam.nrv.data

import scala.collection.mutable.HashMap
import com.wajam.nrv.service.{ActionURL, Endpoints}

/**
 * Base message used for outbound and inbound requests.
 */
abstract class Message(data: Iterable[(String, Any)]) extends HashMap[String, Any] with Serializable {
  var protocolName = ""
  var serviceName = ""
  var path = "/"
  var rendezvous = 0
  var destination = Endpoints.empty

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
    other.destination = this.destination
  }
}
