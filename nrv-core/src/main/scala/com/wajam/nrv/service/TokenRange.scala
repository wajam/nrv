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
  def apply(ranges: Seq[TokenRange]): TokenRangeSeq = new TokenRangeSeq {
    def head = ranges.head

    def find(token: Long) = ranges.find(_.contains(token))

    def next(range: TokenRange) = range.nextRange(ranges)

    def toIterator = ranges.toIterator
  }

  /**
   * Creates a token range sequence wrapper chunked into multiple virtual subranges
   */
  def apply(ranges: Seq[TokenRange], chunkSize: Long): TokenRangeSeq = {
    new ChunkedTokenRangeSeq(TokenRangeSeq(ranges), chunkSize)
  }

  /**
   * A token range sequence wrapper chunked into multiple virtual subranges
   */
  private class ChunkedTokenRangeSeq(ranges: TokenRangeSeq, chunkSize: Long) extends TokenRangeSeq {
    def head = TokenRange(ranges.head.start, math.min(ranges.head.end, ranges.head.start + chunkSize))

    def find(token: Long): Option[TokenRange] = {
      ranges.find(token) match {
        case Some(outerRange) => getChunk(token, outerRange)
        case None => None
      }
    }

    def next(range: TokenRange): Option[TokenRange] = {
      ranges.find(range.start) match {
        case Some(currentOuterRange) if getChunk(range.end, currentOuterRange) == Some(range) => {
          if (currentOuterRange.contains(range.end + 1)) {
            getChunk(range.end + 1, currentOuterRange)
          } else {
            ranges.next(range).map(nextOuterRange => {
              val start = nextOuterRange.start
              val end = math.min(nextOuterRange.end, start + chunkSize)
              TokenRange(start, end)
            })
          }
        }
        case _ => None
      }
    }

    private def getChunk(token: Long, outerRange: TokenRange): Option[TokenRange] = {
      if (outerRange.contains(token)) {
        val outerSize = outerRange.end - outerRange.start
        val multiple = math.floor((token - outerRange.start) / outerSize * chunkSize).toInt
        val start = multiple * chunkSize + outerRange.start
        val end = math.min(start + chunkSize, outerRange.end)
        Some(TokenRange(start, end))
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