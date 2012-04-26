package com.wajam.nrv.service

/**
 *
 */

class ActionMethod(val method: String) {

  def matchMethod(method: ActionMethod) = {
    if(method.equals(ActionMethod.ANY) || this.equals(ActionMethod.ANY)) {
      true
    } else {
      this.equals(method)
    }
  }

  override def equals(other: Any) = {
    if (!other.isInstanceOf[ActionMethod]) {
      false
    } else {
      method.equalsIgnoreCase(other.asInstanceOf[ActionMethod].method)
    }
  }
}

object ActionMethod {

  val ANY = ActionMethod("")

  def apply(method: String) = {
    new ActionMethod(method)
  }

  implicit def string2method(value:String) = ActionMethod(value)
  implicit def method2string(value:ActionMethod) = value.method
}
