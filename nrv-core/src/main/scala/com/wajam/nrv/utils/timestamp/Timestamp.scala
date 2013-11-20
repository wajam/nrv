package com.wajam.nrv.utils.timestamp

import scala.language.implicitConversions

/**
 * Describe a timestamp
 */
trait Timestamp extends Serializable with Ordered[Timestamp] {
  def value: Long

  def timeMs: Long = (value - value % 10000) / 10000

  def seq: Int = (value % 10000).toInt

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
  val MinSeq = 0
  val MaxSeq = 9999

  def apply(tsValue: Long): Timestamp = new Timestamp {
    val value = tsValue
  }

  def apply(ts: Timestamp): Timestamp = Timestamp(ts.value)

  def apply(timeMs: Long, seq: Int): Timestamp = {
    if (seq > Timestamp.MaxSeq) {
      throw new IndexOutOfBoundsException(s"$seq > $MaxSeq")
    } else if (seq < Timestamp.MinSeq) {
      throw new IndexOutOfBoundsException(s"$seq < $MinSeq")
    }

    Timestamp(timeMs * 10000 + seq)
  }

  implicit def long2Timestamp(l: Long) = Timestamp(l)
}
