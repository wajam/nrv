package com.wajam.nrv.utils.timestamp

import scala.language.implicitConversions

/**
 * Describe a timestamp
 */
trait Timestamp extends Serializable with Ordered[Timestamp] {
  def value: Long

  def timeMs: Long = value / Timestamp.SeqPerMs

  def seq: Int = (value % Timestamp.SeqPerMs).toInt

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
  val SeqPerMs = 10000

  def apply(tsValue: Long): Timestamp = new Timestamp {
    val value = tsValue
  }

  def apply(ts: Timestamp): Timestamp = Timestamp(ts.value)

  def apply(timeMs: Long, seq: Int): Timestamp = {
    if (seq >= SeqPerMs) {
      throw new IndexOutOfBoundsException(s"$seq >= $SeqPerMs")
    } else if (seq < 0) {
      throw new IndexOutOfBoundsException(s"$seq < 0")
    }

    Timestamp(timeMs * SeqPerMs + seq)
  }

  implicit def long2Timestamp(l: Long) = Timestamp(l)
}
