package com.wajam.nrv.data

/**
 * Value container for the types allowed in the serialization process.
 *
 * MValue is the short for MessageValue
 */
sealed trait MValue

final case class MString(value: String) extends MValue
final case class MLong(value: Long) extends MValue
final case class MInt(value: Int) extends MValue
final case class MDouble(value: Double) extends MValue
final case class MBoolean(value: Boolean) extends MValue
final case class MList(values: Iterable[MValue]) extends MValue

object MValue {
  implicit def stringToMValue(s: String) = MString(s)
  implicit def longToMValue(l: Long) = MLong(l)
  implicit def intToMValue(l: Int) = MInt(l)
  implicit def doubleToMValue(l: Double) = MDouble(l)
  implicit def booleanToMValue(l: Boolean) = MBoolean(l)
  implicit def iterableToMValue(l: Iterable[MValue]) = MList(l)
}
