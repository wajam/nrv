package com.wajam.nrv.consistency.persistence

import org.scalatest.{Assertions, FunSuite}
import org.scalatest.matchers.ShouldMatchers._
import com.wajam.nrv.data.{Message, InMessage}
import com.wajam.nrv.utils.timestamp.Timestamp
import java.io.IOException
import util.Random

class TestTransactionEvent extends TestTransactionBase {

  test("should write and read message event") {
    val message = new InMessage(Map(("one" -> 1), ("two" -> "II")), Map("c" -> "C"), Map("d" -> "dddd"))

    val expected = TransactionEvent(Timestamp(100), Some(Timestamp(0)), 123, message)
    val buff = expected.writeToBytes()
    val actual = TransactionEvent(buff)

    assertEquals(expected, actual)
  }

  test("should write and read message event without a previous timestamp") {
    val expected = TransactionEvent(Timestamp(100), None, 123, new InMessage())
    val buff = expected.writeToBytes()
    val actual = TransactionEvent(buff)

    assertEquals(expected, actual)
  }

  test("reading a truncated buffer should fail") {
    evaluating {
      TransactionEvent(new Array[Byte](0))
    } should produce[IOException]

    evaluating {
      val buff = createTransaction(1).writeToBytes()
      TransactionEvent(buff.slice(0, buff.length - 1))
    } should produce[IOException]
  }

  test("reading random crap should fail with an IOException") {
    for (i <- 1.to(1000)) {
      evaluating {
        val buff = createTransaction(1).writeToBytes()
        Random.nextBytes(buff)
        TransactionEvent(buff)
      } should produce[IOException]
    }
  }
}

