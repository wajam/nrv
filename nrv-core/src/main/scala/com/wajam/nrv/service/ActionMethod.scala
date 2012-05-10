package com.wajam.nrv.service

/**
 * Method associated to an action, similar to HTTP method
 */
class ActionMethod(val method: String) extends Serializable {

  def matchMethod(method: ActionMethod) = {
    if (method.equals(ActionMethod.ANY) || this.equals(ActionMethod.ANY)) {
      true
    } else {
      this.equals(method)
    }
  }

  override def equals(other: Any) = other match {
    case otherMethod:ActionMethod =>
      method.equalsIgnoreCase(otherMethod.method)
    case _ =>
      false
  }

  override def toString = this.method
}

object ActionMethod {

  val ANY = ActionMethod("")
  val GET = ActionMethod("GET")
  val POST = ActionMethod("POST")
  val PUT = ActionMethod("PUT")
  val DELETE = ActionMethod("DELETE")

  def apply(method: String) = {
    new ActionMethod(method)
  }

  implicit def string2method(value: String) = ActionMethod(value)

  implicit def method2string(value: ActionMethod) = value.method
}
