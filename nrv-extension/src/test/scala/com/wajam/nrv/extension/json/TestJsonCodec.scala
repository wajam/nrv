package com.wajam.nrv.extension.json

import org.scalatest.FunSuite
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import com.wajam.nrv.extension.json.codec.JsonCodec

@RunWith(classOf[JUnitRunner])
class TestJsonCodec extends FunSuite {

  val codec = new JsonCodec

  test("should encode Map to json") {
    val json = Map("key" -> "value")
    val bytes = codec.encode(json, "UTF-8")

    val parsedJson = codec.decode(bytes, "UTF-8")
    assert((parsedJson \\ "key").values.equals("value"))
  }

  test("should be able to encode again a decoded map with Long value") {
    val expected = Map("key" -> 100000000000000L)
    val encoded1 = codec.encode(expected, "UTF-8")
    val decoded1 = codec.decode(encoded1, "UTF-8")
    val actual1 = decoded1.values.asInstanceOf[Map[String, Any]]
    actual1 === expected

    val encoded2 = codec.encode(actual1, "UTF-8")
    val decoded2 = codec.decode(encoded2, "UTF-8")
    val actual2 = decoded2.values.asInstanceOf[Map[String, Any]]
    actual2 === expected
  }
}
