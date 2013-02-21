package com.wajam.nrv.data

/**
 * Value container for the types allowed in the serialization process.
 *
 * MValue is the short for MessageValue
 */
sealed trait MValue

sealed case class MList(values: Iterable[MValue]) extends MValue
sealed case class MString(value: String) extends MValue
sealed case class MLong(value: Long) extends MValue
sealed case class MInt(value: Int) extends MValue
sealed case class MDouble(value: Double) extends MValue
sealed case class MBoolean(value: Boolean) extends MValue
sealed case class MMigrationCatchAll(value: Any) extends MValue // TODO: PBMigration Remove

object MValue {

  implicit def stringToMValue(s: String) = MString(s)
  implicit def longToMValue(l: Long) = MLong(l)
  implicit def intToMValue(l: Int) = MInt(l)
  implicit def doubleToMValue(l: Double) = MDouble(l)
  implicit def booleanToMValue(l: Boolean) = MBoolean(l)

  // Helper to convert usage

  def mapToMMigrationCatchAll(map: Iterable[(String, Any)]): Iterable[(String, MValue)] = {
    map.map { case(k: String, v: Any) => (k, MMigrationCatchAll(v)) }
  }

  implicit def mapToMString(map: Iterable[(String, String)]): Iterable[(String, MValue)] = {
    map.map { case(k: String, v: String) => (k, MString(v)) }
  }
}
