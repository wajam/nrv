package com.wajam.nrv.service

import java.util.zip.CRC32
import com.wajam.nrv.data.{Message, OutMessage}
import util.Random

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
      message.destination = this.resolve(action.service, extractToken(action, message))
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

  def extractToken(action: Action, message: Message) = {
    tokenExtractor.apply(action.path, message.path)
  }
}

object Resolver {
  def TOKEN_FULLPATH = (actionPath: ActionPath, path: String) => hashData(path)

  def TOKEN_HASH_PARAM(param: String) = (actionPath: ActionPath, path: String) => {
    val (_, d) = actionPath.matchesPath(path)
    hashData(d(param))
  }

  def TOKEN_PARAM(param: String) = (actionPath: ActionPath, path: String) => {
    val (_, d) = actionPath.matchesPath(path)
    d(param).toLong
  }

  def TOKEN_RANDOM() = (actionPath: ActionPath, path: String) => {
    random.nextInt().toLong & 0xffffffffL
  }

  def SORTER_RING = null
  def CONSTRAINT_NONE = null

  private val random = new Random()

  def hashData(data: String): Long = {
    val generator = new CRC32()
    generator.update(data.getBytes("UTF-8"))
    generator.getValue
  }
}

trait ResolverConstraint {
  def memberMatches(action: Action, message: OutMessage, currentMatches: Seq[ServiceMember], serviceMember: ServiceMember): Boolean
}


