package com.wajam.nrv.utils.timestamp

/**
 * Describe a timestamp
 */
trait Timestamp extends Serializable with Ordered[Timestamp] {
  def value: Long

  override def compare(t: Timestamp) = compareTo(t)

  override def compareTo(t: Timestamp): Int = {
    value.compareTo(t.value)
  }

  override def equals(other: Any): Boolean =
    other match {
      case that: Timestamp => (that canEqual this) && value == that.value
      case _ => false
    }

  def canEqual(other: Any): Boolean = other.isInstanceOf[Timestamp]

  override def hashCode = value.hashCode()

  override def toString: String = value.toString
}

object Timestamp {
  def apply(l: Long): Timestamp = new Timestamp {
    val value = l
  }
}