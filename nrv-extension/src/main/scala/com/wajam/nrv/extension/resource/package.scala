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

}
