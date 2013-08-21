package com.wajam.nrv.consistency.persistence

import annotation.tailrec

/**
 * A transaction log iterator which wrap multiple iterators iterable in sequence. The underlying iterators are consumed
 * as needed.
 */
class CompositeTransactionLogIterator(iterators: Iterator[LogRecord]*) extends TransactionLogIterator {

  def this(record: LogRecord, iterator: Iterator[LogRecord]) = this(List(record).toIterator, iterator)

  private var itr = iterators.foldLeft(Iterator[LogRecord]())((a, b) => a ++ b)

  def hasNext = itr.hasNext

  def next() = itr.next()

  def close() {
    iterators.foreach(_ match {
      case it: TransactionLogIterator => it.close()
      case _ =>
    })
  }
}
