package com.wajam.nrv.extension

import com.wajam.nrv.service.{Action, Service}
import com.wajam.nrv.data._
import com.wajam.nrv.data.MString
import com.wajam.nrv.data.MList

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

    def getParamValues(name: String): Option[Seq[String]] = {
      request.parameters.get(name).map {
        case MList(strSeq) if strSeq.forall(_.isInstanceOf[MString]) => strSeq.map(_.asInstanceOf[MString].value).toSeq
        case MString(str) => Seq(str)
        case x => throw new RuntimeException("Parameter value has an unsupported type: " + x.getClass)
      }
    }

    def getParamValue(name: String): Option[String] = {
      getParamValues(name).flatMap(_.headOption)
    }

    def getParamValue(name: String, default: String): String = {
      getParamValue(name).getOrElse(default)
    }

    def getParamValue[DefaultType](name: String, f: String => DefaultType, default: DefaultType): DefaultType = {
      getParamValue(name).map(f).getOrElse(default)
    }

    def respond(response: Any, headers: Map[String, MValue] = Map(), code: Int = 200) {
      request.reply(headers, meta = null, data = response, code = code)
    }

  }


}
