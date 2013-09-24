package com.wajam.nrv.extension.resource

import com.wajam.nrv.service.ActionMethod

/**
 * Trait that defines a resource with all the optional operations {GET, INDEX, CREATE, UPDATE, DELETE}.
 */
trait Resource {

  /**
   * Name of this resource.
   */
  def resourceName: String

  /**
   * The name of the resource identifier in the ActionPath.
   */
  def idName: String

  /**
   * The parent resource if any.
   */
  def parent: Option[Resource]

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

  private lazy val path: String = parent.map(p => p.pathWithId).getOrElse("") + "/" + resourceName
  private lazy val pathWithId: String = path + "/:" + idName

  /**
   * Defines all operations supported by this resource.
   */
  private[resource] lazy val operations = {
    (for (getOp <- get) yield OperationDefinition(ActionMethod.GET, pathWithId, getOp)) ++
      (for (listOp <- list) yield OperationDefinition(ActionMethod.GET, path, listOp)) ++
      (for (createOp <- create) yield OperationDefinition(ActionMethod.POST, path, createOp)) ++
      (for (updateOp <- update) yield OperationDefinition(ActionMethod.PUT, pathWithId, updateOp)) ++
      (for (deleteOp <- delete) yield OperationDefinition(ActionMethod.DELETE, pathWithId, deleteOp))
  }

}

/**
 * Defines resource operation parameters [method, path, operation].
 */
private[resource] case class OperationDefinition(method: ActionMethod, path: String, operation: (Request) => Unit)
