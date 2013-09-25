package com.wajam.nrv.extension

import com.wajam.nrv.service.{Action, Service}
import com.wajam.nrv.data._
import com.wajam.nrv.data.MList
import com.wajam.nrv.InvalidParameter

/**
 * The resource package defines traits and classes that are useful when creating a NRV service based on
 * resources (REST like service).
 */
package object resource {

  /**
   * Implicit class to add methods on a NRV service to register Resources to the service.
   */
  implicit class ResourcefulService(service: Service) {

    /**
     * Register a resource with all the defined operations.
     * @param resource The resource to register
     */
    def registerResource(resource: Resource) {
      resource.registerTo(service)
    }

    /**
     * Register a list of resources with all their defined operations.
     * @param resources The list of resources to register
     */
    def registerResources(resources: Resource*) {
      for(r <- resources) {
        registerResource(r)
      }
    }

    /**
     * Retrieve the corresponding GET Action for the given resource registered to a given service.
     */
    def get(resource: Resource with Get): Option[Action] = resource.get(service)

    /**
     * Retrieve the corresponding LIST Action for the given resource registered to a given service.
     */
    def list(resource: Resource with List): Option[Action] = resource.list(service)

    /**
     * Retrieve the corresponding CREATE Action for the given resource registered to a given service.
     */
    def create(resource: Resource with Create): Option[Action] = resource.create(service)

    /**
     * Retrieve the corresponding UPDATE Action for the given resource registered to a given service.
     */
    def update(resource: Resource with Update): Option[Action] = resource.update(service)

    /**
     * Retrieve the corresponding DELETE Action for the given resource registered to a given service.
     */
    def delete(resource: Resource with Delete): Option[Action] = resource.delete(service)

  }

  /**
   * Implicit class to add convenience methods to an InMessage.
   */
  implicit class Request(protected val request: InMessage) {

    def path = request.path

    def token = request.token

    def paramString(param: String): String = {
      paramString(param, throw new InvalidParameter(s"Parameter $param must be specified"))
    }

    def paramString(param: String, default: => String): String = paramOptionalString(param).getOrElse(default)

    def paramOptionalString(param: String): Option[String] = paramOptionalSeqString(param) match {
      case Some(head :: _) => Some(head)
      case _ => None
    }

    def paramOptionalSeqString(param: String): Option[Seq[String]] = request.parameters.get(param).map {
      case MList(values) => values.map(_.toString).toSeq
      case value: MValue => Seq(value.toString)
      case value => throw new InvalidParameter(s"Parameter $param unsupported value $value")
    }

    def paramBoolean(param: String): Boolean = {
      paramBoolean(param, throw new InvalidParameter(s"Parameter $param must be specified"))
    }

    def paramBoolean(param: String, default: => Boolean): Boolean = paramOptionalBoolean(param).getOrElse(default)

    def paramOptionalBoolean(param: String): Option[Boolean] = paramOptionalString(param).map {
      case "1" => true
      case "0" => false
      case "true" => true
      case "false" => false
      case "t" => true
      case "f" => false
      case value => throw new InvalidParameter(s"Parameter $param unsupported value $value")
    }

    def paramLong(param: String): Long = {
      paramLong(param, throw new InvalidParameter(s"Parameter $param must be specified"))
    }

    def paramLong(param: String, default: => Long): Long = paramOptionalLong(param).getOrElse(default)

    def paramOptionalLong(param: String): Option[Long] = {
      try {
        paramOptionalString(param).map(_.toLong)
      } catch {
        case e: Exception => throw new InvalidParameter(s"Parameter $param must be numeric")
      }
    }

    def paramInt(param: String): Int = {
      paramInt(param, throw new InvalidParameter(s"Parameter $param must be specified"))
    }

    def paramInt(param: String, default: => Int): Int = paramOptionalInt(param).getOrElse(default)

    def paramOptionalInt(param: String): Option[Int] = {
      val value = paramOptionalLong(param)
      try {
        value.map(_.toInt)
      } catch {
        case e: Exception => throw new InvalidParameter(s"Parameter $param unsupported value $value")
      }
    }

    def respond(response: Any, headers: Map[String, MValue] = Map(), code: Int = 200) {
      request.reply(headers, meta = null, data = response, code = code)
    }

  }


}
