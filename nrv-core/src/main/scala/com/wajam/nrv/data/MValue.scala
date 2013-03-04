package com.wajam.nrv.data

/**
 * Value container for the types allowed in the serialization process.
 *
 * MValue is the short for MessageValue
 */
sealed trait MValue

final case class MList(values: Iterable[MValue]) extends MValue

final case class MString(value: String) extends MValue {
  override def toString() = {
    value.toString
  }
}

final case class MLong(value: Long) extends MValue {
  override def toString() = {
    value.toString
  }
}

final case class MInt(value: Int) extends MValue {
  override def toString() = {
    value.toString
  }
}

final case class MDouble(value: Double) extends MValue {
  override def toString() = {
    value.toString
  }
}
final case class MBoolean(value: Boolean) extends MValue {
  override def toString() = {
    value.toString
  }
}

object MValue {

  implicit def stringToMValue(s: String) = MString(s)
  implicit def longToMValue(l: Long) = MLong(l)
  implicit def intToMValue(l: Int) = MInt(l)
  implicit def doubleToMValue(l: Double) = MDouble(l)
  implicit def booleanToMValue(l: Boolean) = MBoolean(l)

  // Helper to convert usage

  implicit def mapToMString(map: Iterable[(String, String)]): Iterable[(String, MValue)] = {
    map.map { case(k: String, v: String) => (k, MString(v)) }
  }
}
