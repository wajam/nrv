package com.wajam.nrv.consistency.persistence

import org.scalatest.matchers.ShouldMatchers._
import com.wajam.nrv.data.InMessage
import com.wajam.nrv.utils.timestamp.Timestamp
import java.io.IOException
import util.Random
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import com.wajam.nrv.data.MValue._

@RunWith(classOf[JUnitRunner])
class TestTransactionEventSerializer extends TestTransactionBase {

  test("should serialize and deserialize message event") {
    val serializer = new TransactionEventSerializer
    val message = new InMessage(Map(("one" -> 1), ("two" -> "II")), Map("c" -> "C"), Map("d" -> "dddd"))

    val expected = TransactionEvent(Timestamp(100), Some(Timestamp(0)), 123, message)
    val buff = serializer.serialize(expected)
    val actual = serializer.deserialize(buff)

    assertEquals(expected, actual)
  }

  test("should serialize and deserialize message event without a previous timestamp") {
    val serializer = new TransactionEventSerializer

    val expected = TransactionEvent(Timestamp(100), None, 123, new InMessage())
    val buff = serializer.serialize(expected)
    val actual = serializer.deserialize(buff)

    assertEquals(expected, actual)
  }

  test("deserializing a truncated buffer should fail") {
    val serializer = new TransactionEventSerializer

    evaluating {
      serializer.deserialize(new Array[Byte](0))
    } should produce[IOException]

    evaluating {
      val buff = serializer.serialize(createTransaction(1))
      serializer.deserialize(buff.slice(0, buff.length - 1))
    } should produce[IOException]
  }

  test("deserializing random crap should fail with an IOException") {
    val serializer = new TransactionEventSerializer

    for (i <- 1.to(1000)) {
      evaluating {
        val buff = serializer.serialize(createTransaction(1))
        Random.nextBytes(buff)
        serializer.deserialize(buff)
      } should produce[IOException]
    }
  }
}

