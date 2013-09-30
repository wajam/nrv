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
      for (r <- resources) {
        registerResource(r)
      }
    }

    /**
     * Retrieve the corresponding GET Action for the given resource registered to a given service.
     */
    def get(resource: Resource with Get): Option[Action] = resource.get(service)

    /**
     * Retrieve the corresponding INDEX Action for the given resource registered to a given service.
     */
    def index(resource: Resource with Index): Option[Action] = resource.index(service)

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

    import Request._

    def path = request.path

    def token = request.token

    /**
     * Get the value for the given optional parameter name.
     *
     * @param name The parameter name
     * @param extractor The extracting partial function that convert a MValue to a type T
     * @tparam T The type of the parameter value
     * @return the value of the parameter
     */
    def optionalParam[T](name: String)(implicit extractor: Extractor[T]): Option[T] = {
      request.parameters.get(name).map(extractor orElse fail(name))
    }

    /**
     * Get the value for the given parameter name or the specified default value.
     *
     * @param name The parameter name
     * @param extractor The extracting partial function that convert a MValue to a type T
     * @param default The default value if the parameter is not specified in the message.
     * @tparam T The type of the parameter value
     * @return the value of the parameter
     */
    def param[T](name: String, default: => T)(implicit extractor: Extractor[T]): T = {
      optionalParam(name)(extractor).getOrElse(default)
    }

    /**
     * Get the value for the given parameter name or throw an InvalidParameter exception is the
     * parameter is not set in the message.
     *
     * @param name The parameter name
     * @param extractor The extracting partial function that convert a MValue to a type T
     * @tparam T The type of the parameter value
     * @return the value of the parameter
     */
    def param[T](name: String)(implicit extractor: Extractor[T]): T = {
      param[T](name, throw new InvalidParameter(s"Parameter $name must be specified"))(extractor)
    }

    /**
     * Respond to the request.
     *
     * @param response The response body
     * @param headers The response HTTP headers
     * @param code The response code
     */
    def respond(response: Any, headers: Map[String, MValue] = Map(), code: Int = 200) {
      request.reply(Map(), headers, response, code)
    }

  }

  object Request {

    /**
     * Message parameter value extractor. Given a MValue, try to extract a T
     */
    type Extractor[T] = PartialFunction[MValue, T]

    /**
     * Extractor for String value. Convert the MValue to a String or, in the case of MList, convert the head to a String
     */
    implicit def stringExtractor: Extractor[String] = {
      case MList(head :: _) => head.toString
      case other => other.toString
    }

    /**
     * Extractor for Long. Convert MString, MLong or MInt to Long.
     */
    implicit def longExtractor: Extractor[Long] = {
      case MString(s) => s.toLong
      case MLong(l) => l
      case MInt(i) => i.toLong
    }

    /**
     * Extractor for Int. Convert MString or MInt to Int.
     */
    implicit def intExtractor: Extractor[Int] = {
      case MString(s) => s.toInt
      case MInt(i) => i
    }

    /**
     * Extractor for Double. Convert MString, MInt, MLong and MDouble to Double.
     */
    implicit def doubleExtractor: Extractor[Double] = {
      case MString(s) => s.toDouble
      case MInt(i) => i.toDouble
      case MLong(l) => l.toDouble
      case MDouble(d) => d
    }

    /**
     * Extractor for Boolean. Convert MString to Boolean if the contained string is in [1, 0, t, f, true, false]
     * Converts MBoolean to Boolean.
     */
    implicit def booleanExtractor: Extractor[Boolean] = {
      case MString(s) => s.toLowerCase match {
        case "1" | "true" | "t" => true
        case "0" | "false" | "f" => false
        case _ => throw new InvalidParameter(s"$s is not a boolean.")
      }
      case MBoolean(b) => b
    }

    /**
     * Extractor for List[String]. Converts MList to List[String] or wrap any MValue converted to a string in a
     * single element List.
     */
    implicit def listStringExtractor: Extractor[scala.collection.immutable.List[String]] = {
      case MList(values) => values.map(_.toString).toList
      case other => List(other.toString)
    }

    def fail[T](name: String): PartialFunction[MValue, T] = {
      case value => throw new InvalidParameter(s"Parameter $name unsupported value $value")
    }

  }


}
