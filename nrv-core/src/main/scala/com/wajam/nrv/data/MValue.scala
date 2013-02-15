package com.wajam.nrv.data

sealed trait MValue {
  case class NrvString(value: String) extends MValue
  case class NrvLong(value: Long) extends MValue
  case class NrvInt(value: Long) extends MValue
  case class NrvList(values: Iterable[MValue]) extends MValue

  implicit def stringToMValue(s: String) = NrvString(s)
  implicit def longToMValue(l: Long) = NrvLong(l)
  implicit def intToMValue(l: Long) = NrvInt(l)
  implicit def iterableToMValue(l: Iterable[MValue]) = NrvList(l)
}
