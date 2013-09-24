package com.wajam.nrv.extension.resource

import com.wajam.nrv.service.ActionMethod

/**
 * Trait that defines a resource with all the optional operations {GET, INDEX, CREATE, UPDATE, DELETE}.
 */
trait Resource {

  /**
   * Base path defining this resource.
   */
  def basePath: String

  /**
   * The name of the resource identifier in the ActionPath.
   */
  def idName: String

  /**
   * Optional GET operation on the resource.
   */
  def get: Option[(Request) => Unit]

  /**
   * Optional LIST operation on the resource
   */
  def list: Option[(Request) => Unit]

  /**
   * Optional CREATE operation on the resource
   */
  def create: Option[(Request) => Unit]

  /**
   * Optional UPDATE operation on the resource
   */
  def update: Option[(Request) => Unit]

  /**
   * Optional DELETE operation on the resource
   */
  def delete: Option[(Request) => Unit]

  private lazy val pathWithId = basePath + "/:" + idName

  /**
   * Defines all operation supported by this resource.
   */
  private[resource] lazy val operations = {
    (for (getOp <- get) yield OperationDefinition(ActionMethod.GET, pathWithId, getOp)) ++
      (for (indexOp <- get) yield OperationDefinition(ActionMethod.GET, basePath, indexOp)) ++
      (for (createOp <- get) yield OperationDefinition(ActionMethod.POST, basePath, createOp)) ++
      (for (updateOp <- get) yield OperationDefinition(ActionMethod.PUT, pathWithId, updateOp)) ++
      (for (deleteOp <- get) yield OperationDefinition(ActionMethod.DELETE, pathWithId, deleteOp))
  }

}

/**
 * Defines resource operation parameters [method, path, operation].
 */
private[resource] case class OperationDefinition(method: ActionMethod, path: String, operation: (Request) => Unit)
