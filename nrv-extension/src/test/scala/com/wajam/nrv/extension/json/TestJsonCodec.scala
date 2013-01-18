package com.wajam.nrv.extension.json

import org.scalatest.FunSuite
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import com.wajam.nrv.extension.json.codec.JsonCodec

/**
 *
 */

@RunWith(classOf[JUnitRunner])
class TestJsonCodec extends FunSuite {

  val codec = new JsonCodec

  test("should encode Map to json") {
    val json = Map("key" -> "value")
    val bytes = codec.encode(json, "UTF-8")

    val parsedJson = codec.decode(bytes, "UTF-8")
    assert((parsedJson \\ "key").values.equals("value"))
  }


}
