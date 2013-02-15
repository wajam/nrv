package com.wajam.nrv.data

/**
 * Value container for the types allowed in the serialization process.
 *
 * MValue is the short for MessageValue
 */
sealed trait MValue

case class MString(value: String) extends MValue
case class MLong(value: Long) extends MValue
case class MInt(value: Int) extends MValue
case class MList(values: Iterable[MValue]) extends MValue

object MValue {
  implicit def stringToMValue(s: String) = MString(s)
  implicit def longToMValue(l: Long) = MLong(l)
  implicit def intToMValue(l: Int) = MInt(l)
  implicit def iterableToMValue(l: Iterable[MValue]) = MList(l)
}
