package com.wajam.nrv.extension

import com.wajam.nrv.service.{Action, Service}
import com.wajam.nrv.data.{MValue, MString, MList, InMessage}

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
      for(definition <- resource.operations) {
        service.registerAction(new Action(definition.path, (request) => definition.operation(request), method = definition.method))
      }
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

  }

  /**
   * Implicit class to add convenience methods on an InMessage.
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
      getParamValues(name).filter(_.size > 0).map(values => values.head)
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
