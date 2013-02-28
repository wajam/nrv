package com.wajam.nrv.consistency.persistence

import annotation.tailrec

/**
 * A transaction log iterator which wrap multiple iterators iterable in sequence. The underlying iterators are consumed
 * as needed.
 */
class CompositeTransactionLogIterator(iterators: Iterator[LogRecord]*) extends TransactionLogIterator {

  def this(record: LogRecord, iterator: Iterator[LogRecord]) = this(List(record).toIterator, iterator)

  private var itList = iterators.toList

  @tailrec
  final def hasNext = {
    itList match {
      case head :: _ if head.hasNext => true
      case head :: tail => itList = tail; hasNext
      case Nil => false
    }
  }

  def next() = {
    itList match {
      case head :: _ => head.next()
      case Nil => null
    }
  }

  def close() {
    iterators.foreach(_ match {
      case it: TransactionLogIterator => it.close()
      case _ =>
    })
  }
}
