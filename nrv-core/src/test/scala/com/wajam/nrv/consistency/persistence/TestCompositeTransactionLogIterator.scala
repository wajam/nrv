package com.wajam.nrv.consistency.persistence

import org.scalatest.matchers.ShouldMatchers._
import org.mockito.Mockito._
import com.wajam.nrv.consistency.persistence.LogRecord.Index
import com.wajam.nrv.consistency.TestTransactionBase
import org.scalatest.mock.MockitoSugar

class TestCompositeTransactionLogIterator extends TestTransactionBase with MockitoSugar {

  test("should iterate all iterators") {
    val list1 = List(Index(1), Index(2))
    val list2 = List(Index(10))
    val list3 = List(Index(30), Index(31))

    val compositeItr = new CompositeTransactionLogIterator(list1.toIterator, list2.toIterator, list3.toIterator)
    compositeItr.toList should be(list1 ::: list2 ::: list3)

    val emptyItr = List[LogRecord]().toIterator
    val compositeItrWithEmpty = new CompositeTransactionLogIterator(
      emptyItr, list1.toIterator, emptyItr, list2.toIterator, emptyItr, list3.toIterator, emptyItr, emptyItr)
    compositeItrWithEmpty.toList should be(list1 ::: list2 ::: list3)
  }

  test("should iterate all iterators (alternate ctor)") {
    val list = List(Index(10), Index(11))

    val compositeItr = new CompositeTransactionLogIterator(Index(0), list.toIterator)
    compositeItr.toList should be(List(Index(0), Index(10), Index(11)))
  }

  test("should close transaction log iterator") {
    val list = List(Index(1), Index(2))
    val mockTxLogItr = mock[TransactionLogIterator]

    val compositeItr = new CompositeTransactionLogIterator(list.toIterator, mockTxLogItr)
    compositeItr.close()

    verify(mockTxLogItr).close()
    verifyNoMoreInteractions(mockTxLogItr)
  }
}
