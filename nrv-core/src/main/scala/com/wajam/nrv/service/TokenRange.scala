package com.wajam.nrv.service

case class TokenRange(start: Long, end: Long) {
  def contains(token: Long) = token >= start && token <= end

  /**
   * Returns the next range in the specified sequence
   */
  def nextRange(ranges: Seq[TokenRange]): Option[TokenRange] = {
    val remaining = ranges.dropWhile(_ != this)
    remaining match {
      case _ :: n :: _ => Some(n)
      case _ => None
    }
  }
}

object TokenRange {
  val MaxToken = Int.MaxValue.toLong * 2

  val All = TokenRange(0, MaxToken)
}
