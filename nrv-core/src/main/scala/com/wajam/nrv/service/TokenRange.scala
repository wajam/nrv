package com.wajam.nrv.service

import scala.language.implicitConversions

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
  val MaxToken = 0xFFFFFFFFL // (Int.MaxValue.toLong * 2 + 1) or (1l<<32) - 1
  val All = TokenRange(0, MaxToken)
}

/**
 * Represent a sequence of token range
 */
trait TokenRangeSeq {
  /**
   * Returns the first token range of the sequence
   */
  def head: TokenRange

  /**
   * Returns the token range containing the specified token
   */
  def find(token: Long): Option[TokenRange]

  /**
   * Returns the token range following the specified range or None if the specified range is the last range in the
   * sequence or is not contained in this sequence.
   */
  def next(range: TokenRange): Option[TokenRange]

  /**
   * Returns an Iterator over the ranges in this sequence.
   */
  def toIterator: Iterator[TokenRange]
}

object TokenRangeSeq {
  /**
   * Creates a token range sequence wrapper
   */
  implicit def apply(ranges: Seq[TokenRange]): TokenRangeSeq = new TokenRangeSeq {
    def head = ranges.head

    def find(token: Long) = ranges.find(_.contains(token))

    def next(range: TokenRange) = range.nextRange(ranges)

    def toIterator = ranges.toIterator
  }

  implicit def apply(range: TokenRange): TokenRangeSeq = TokenRangeSeq(Seq(range))

  /**
   * Creates a token range sequence wrapper chunked into multiple virtual subranges
   */
  def apply(ranges: Seq[TokenRange], chunkSize: Long): TokenRangeSeq = {
    new ChunkedTokenRangeSeq(TokenRangeSeq(ranges), chunkSize)
  }

  /**
   * A token range sequence wrapper chunked into multiple virtual subranges
   */
  private class ChunkedTokenRangeSeq(seq: TokenRangeSeq, chunkSize: Long) extends TokenRangeSeq {

    require(chunkSize > 0, "chunk size %d must be > 0".format(chunkSize))

    def head = TokenRange(seq.head.start, math.min(seq.head.end, seq.head.start + chunkSize - 1))

    def find(token: Long): Option[TokenRange] = {
      seq.find(token) match {
        case Some(outerRange) => getChunk(token, outerRange)
        case None => None
      }
    }

    def next(range: TokenRange): Option[TokenRange] = {
      seq.find(range.start) match {
        case Some(currentOuterRange) if getChunk(range.end, currentOuterRange) == Some(range) => {
          if (currentOuterRange.contains(range.end + 1)) {
            getChunk(range.end + 1, currentOuterRange)
          } else {
            seq.next(currentOuterRange).map(nextOuterRange => {
              val chunkStart = nextOuterRange.start
              val chunkEnd = math.min(nextOuterRange.end, chunkStart + chunkSize - 1)
              TokenRange(chunkStart, chunkEnd)
            })
          }
        }
        case _ => None
      }
    }

    private def getChunk(token: Long, outerRange: TokenRange): Option[TokenRange] = {
      if (outerRange.contains(token)) {
        val outerSize = outerRange.end - outerRange.start
        if (chunkSize > outerSize) {
          Some(outerRange)
        } else {
          val multiple = (token - outerRange.start) / chunkSize
          val chunkStart = multiple * chunkSize + outerRange.start
          val chunkEnd = math.min(chunkStart + chunkSize - 1, outerRange.end)
          Some(TokenRange(chunkStart, chunkEnd))
        }
      } else None
    }

    def toIterator = new Iterator[TokenRange] {
      var nextChunk: Option[TokenRange] = Some(head)

      def hasNext = nextChunk.isDefined

      def next() = {
        val chunk = nextChunk.get
        nextChunk = ChunkedTokenRangeSeq.this.next(chunk)
        chunk
      }
    }
  }
}