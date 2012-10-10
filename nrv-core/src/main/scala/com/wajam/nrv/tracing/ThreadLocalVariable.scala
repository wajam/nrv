package com.wajam.nrv.tracing

/**
 * Copied from DynamicVariable but do not inherit parent's thread value
 */
class ThreadLocalVariable[T] (init: T) {
  private val tl = new InheritableThreadLocal[T] {

    //child do not inherit parent thread value
    override def childValue(parentValue: T) = initialValue

    override def initialValue = init.asInstanceOf[T with AnyRef]
  }

  /** Retrieve the current value */
  def value: T = tl.get.asInstanceOf[T]

  /** Set the value of the variable while executing the specified
    * thunk.
    *
    * @param newval The value to which to set the variable
    * @param thunk The code to evaluate under the new setting
    */
  def withValue[S](newval: T)(thunk: => S): S = {
    val oldval = value
    tl set newval

    try thunk
    finally tl set oldval
  }

  /** Change the currently bound value, discarding the old value.
    * Usually withValue() gives better semantics.
    */
  def value_=(newval: T) = tl set newval

  override def toString: String = "DynamicVariable(" + value + ")"
}
