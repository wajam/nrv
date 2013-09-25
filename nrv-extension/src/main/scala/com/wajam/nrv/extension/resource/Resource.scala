package com.wajam.nrv.extension.resource

import com.wajam.nrv.service.{ActionMethod, Action, Service}
import com.wajam.nrv.data.InMessage

/**
 * Class that defines a resource.
 *
 * Operations on that resource are added by mixing-in one or more of the Operation traits.
 */
class Resource(resourceName: String, idName: String, parent: Option[Resource] = None) extends Operation {
  private[resource] lazy val path: String = parent.map(p => p.pathWithId).getOrElse("") + "/" + resourceName
  private[resource] lazy val pathWithId: String = path + "/:" + idName
}

/**
 * Base trait for resources and resource operations.
 *
 * The registerTo method follows the stackable trait pattern. It delegates the registration of the operation actions
 * to the specific operation trait.
 */
sealed trait Operation {

  /**
   * Register the Resource operation to a Service.
   */
  def registerTo(service: Service): Unit = {}
}

/**
 * The following traits define GET, LIST, CREATE, UPDATE, DELETE operations.
 *
 * The have to be mixed-in to a Resource.
 */

trait Get extends Operation {
  this: Resource =>

  def get(service: Service): Option[Action] = service.findAction(pathWithId, ActionMethod.GET)

  protected def get: (Request) => Unit

  abstract override def registerTo(service: Service): Unit = {
    service.registerAction(new Action(pathWithId, (message: InMessage) => get(message), method = ActionMethod.GET))
    super.registerTo(service)
  }

}

trait List extends Operation {
  this: Resource =>

  def list(service: Service): Option[Action] = service.findAction(path, ActionMethod.GET)

  protected def list: (Request) => Unit

  abstract override def registerTo(service: Service): Unit = {
    service.registerAction(new Action(path, (message: InMessage) => list(message), method = ActionMethod.GET))
    super.registerTo(service)
  }

}

trait Create extends Operation {
  this: Resource =>

  def create(service: Service): Option[Action] = service.findAction(path, ActionMethod.POST)

  protected def create: (Request) => Unit

  abstract override def registerTo(service: Service): Unit = {
    service.registerAction(new Action(path, (message: InMessage) => create(message), method = ActionMethod.POST))
    super.registerTo(service)
  }

}

trait Update extends Operation {
  this: Resource =>

  def update(service: Service): Option[Action] = service.findAction(pathWithId, ActionMethod.PUT)

  protected def update: (Request) => Unit

  abstract override def registerTo(service: Service): Unit = {
    service.registerAction(new Action(pathWithId, (message: InMessage) => update(message), method = ActionMethod.PUT))
    super.registerTo(service)
  }

}

trait Delete extends Operation {
  this: Resource =>

  def delete(service: Service): Option[Action] = service.findAction(pathWithId, ActionMethod.DELETE)

  protected def delete: (Request) => Unit

  abstract override def registerTo(service: Service): Unit = {
    service.registerAction(new Action(pathWithId, (message: InMessage) => delete(message), method = ActionMethod.DELETE))
    super.registerTo(service)
  }

}