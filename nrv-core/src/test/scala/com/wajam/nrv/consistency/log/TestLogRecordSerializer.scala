package com.wajam.nrv.consistency.log

import org.scalatest.Matchers._
import java.io.IOException
import util.Random
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import com.wajam.nrv.consistency.TestTransactionBase
import com.wajam.nrv.consistency.log.LogRecord.{Response, Request}
import com.wajam.nrv.utils.timestamp.Timestamp

@RunWith(classOf[JUnitRunner])
class TestLogRecordSerializer extends TestTransactionBase {

  test("should serialize and deserialize request record") {
    val serializer = new LogRecordSerializer
    val message = createRequestMessage(timestamp = 100, token = 200)

    val id = 3L
    val expected = Request(id, Some(Timestamp(0)), message)
    val buff = serializer.serialize(expected)
    val actual = serializer.deserialize(buff)

    actual should be(expected)
  }

  test("serializing a message > MaxMessageLen should fail") {
    val serializer = new LogRecordSerializer
    val data = new Array[Byte](10000)
    val request = Request(id = 3L, None, createRequestMessage(timestamp = 100, data = data))

    evaluating {
      serializer.serialize(request, maxMessageLen = data.length)
    } should produce[IllegalArgumentException]
  }

  test("should serialize and deserialize request record event without a consistant timestamp") {
    val serializer = new LogRecordSerializer
    val message = createRequestMessage(timestamp = 100, token = 200)

    val id = 3L
    val expected = Request(id, None, message)
    val buff = serializer.serialize(expected)
    val actual = serializer.deserialize(buff)

    actual should be(expected)
  }

  test("should serialize and deserialize success response record") {
    val serializer = new LogRecordSerializer
    val message = createResponseMessage(createRequestMessage(timestamp = 100, token = 200))

    val id = 3L
    val expected = Response(id, Some(Timestamp(0)), message)
    expected.status should be(Response.Success)
    val buff = serializer.serialize(expected)
    val actual = serializer.deserialize(buff)

    actual should be(expected)
  }

  test("should serialize and deserialize error response record") {
    val serializer = new LogRecordSerializer
    val message = createResponseMessage(createRequestMessage(timestamp = 100, token = 200), code = 500)

    val id = 3L
    val expected = Response(id, Some(Timestamp(0)), message)
    expected.status should be(Response.Error)
    val buff = serializer.serialize(expected)
    val actual = serializer.deserialize(buff)

    actual should be(expected)
  }

  test("should serialize and deserialize response record event without a consistant timestamp") {
    val serializer = new LogRecordSerializer
    val message = createResponseMessage(createRequestMessage(timestamp = 100, token = 200),
      error = new Some(new Exception))

    val id = 3L
    val expected = Response(id, None, message)
    expected.status should be(Response.Error)
    val buff = serializer.serialize(expected)
    val actual = serializer.deserialize(buff)

    actual should be(expected)
  }

  test("deserializing a truncated record buffer should fail") {
    val serializer = new LogRecordSerializer

    evaluating {
      serializer.deserialize(new Array[Byte](0))
    } should produce[IOException]

    evaluating {
      val record = Request(3L, None, createRequestMessage(timestamp = 100, token = 200))
      val buff = serializer.serialize(record)
      serializer.deserialize(buff.slice(0, buff.length - 1))
    } should produce[IOException]
  }

  test("deserializing random crap should fail with an exception") {
    val serializer = new LogRecordSerializer
    val random = new Random(0L)

    for (i <- 1.to(1000)) {
      evaluating {
        val record = LogRecord(id = 3L, None, createRequestMessage(timestamp = 100, token = 200))
        val buff = serializer.serialize(record)
        random.nextBytes(buff)
        serializer.deserialize(buff)
      } should produce[Exception]
    }
  }
}

