package com.wajam.nrv.utils

/**
 * Trait for creating identifier.
 */
trait IdGenerator[T] {
  def nextId: T
}

/**
 * This trait should be used as a mixin to synchronize id generation for the class it is mixed in.
 */
trait SynchronizedIdGenerator[T] extends IdGenerator[T] {
  abstract override def nextId: T = synchronized {
    super.nextId
  }
}
