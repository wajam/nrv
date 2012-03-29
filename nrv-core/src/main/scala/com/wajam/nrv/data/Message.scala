package com.wajam.nrv.data

import collection.mutable.HashMap
import com.wajam.nrv.service.Endpoints

/**
 * Base message used for outbound and inbound requests.
 */
abstract class Message(data: Iterable[(String, Any)]) extends HashMap[String, Any] with Serializable {
  var protocolName: String = ""
  var serviceName: String = ""
  var path: String = ""
  var destination: Endpoints = Endpoints.empty

  loadData(data)

  def this(params: (String, Any)*) = this(params)

  def loadData(data: Iterable[(String, Any)]) {
    this ++= data
  }

  def copyTo(other: Message) {
    other.loadData(this)
    other.protocolName = this.protocolName
    other.serviceName = this.serviceName
    other.path = this.path
    other.destination = this.destination
  }
}
