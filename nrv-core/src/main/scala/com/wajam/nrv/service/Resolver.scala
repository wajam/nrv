package com.wajam.nrv.service

import java.util.zip.CRC32
import com.wajam.nrv.data.Message

/**
 * Resolves endpoints that handle a specific action (from a path) within a service.
 * Resolver always resolve all replica endpoints.
 */
class Resolver(var replica: Option[Int] = Some(1)) {
  def handleIncoming(action: Action, message: Message) {
  }

  def handleOutgoing(action: Action, message: Message) {
    message.destination = this.resolve(action, message.path)
  }

  def resolve(action: Action, path: String): Endpoints = {
    // use hashed path to resolve the node that will handle the call
    val results = action.service.resolveMembers(Resolver.hashData(path), replica.get)

    var endpointsList = List[ServiceMember]()
    for (result <- results)
      endpointsList ::= new ServiceMember(result.token, result.value.get)

    new Endpoints(endpointsList)
  }
}

object Resolver {
  private val crcGenerator = new CRC32()

  def hashData(data: String): Long = {
    crcGenerator.reset()
    crcGenerator.update(data.getBytes("UTF-8"))
    crcGenerator.getValue
  }
}
