package com.wajam.nrv.service

import java.util.zip.CRC32
import com.wajam.nrv.data.OutMessage

/**
 * Resolves endpoints that handle a specific action (from a path) within a service.
 * Resolver always resolve all replica endpoints.
 */
class Resolver(val replica: Int = 1,
               val tokenExtractor: (ActionPath, String) => Long = Resolver.TOKEN_FULLPATH,
               val constraints: (List[ServiceMember], ServiceMember) => Boolean = Resolver.CONSTRAINT_NONE,
               val sorter: (ServiceMember, ServiceMember) => Boolean = Resolver.SORTER_RING) extends MessageHandler {

  override def handleOutgoing(action: Action, message: OutMessage) {
    if (message.destination.size == 0)
      message.destination = this.resolve(action.service, tokenExtractor(action.path, message.path))
  }

  def resolve(service: Service, token: Long): Endpoints = {
    var endpointsList = List[ServiceMember]()
    if (constraints == Resolver.CONSTRAINT_NONE) {
      val results = service.resolveMembers(token, replica)
      for (result <- results) {
        endpointsList :+= new ServiceMember(result.token, result.value.get)
      }
    } else {
      service.resolveMembers(token, replica, member => {
        val toAdd = constraints(endpointsList, member)
        if (toAdd)
          endpointsList :+= member
        toAdd
      })
    }

    if (sorter != null)
      endpointsList = endpointsList.sortWith(sorter)

    new Endpoints(endpointsList)
  }
}

object Resolver {
  def TOKEN_FULLPATH = (actionPath: ActionPath, path: String) => hashData(path)

  def TOKEN_PARAM(param: String) = (actionPath: ActionPath, path: String) => {
    val (_, d) = actionPath.matchesPath(path)
    hashData(d(param))
  }

  def SORTER_RING = null

  def CONSTRAINT_NONE = null


  private val crcGenerator = new CRC32()

  def hashData(data: String): Long = {
    crcGenerator.reset()
    crcGenerator.update(data.getBytes("UTF-8"))
    crcGenerator.getValue
  }
}

trait ResolverConstraint {
  def memberMatches(action: Action, message: OutMessage, currentMatches: Seq[ServiceMember], serviceMember: ServiceMember): Boolean
}


