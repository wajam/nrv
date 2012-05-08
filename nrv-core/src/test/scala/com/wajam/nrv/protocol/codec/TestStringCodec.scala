package com.wajam.nrv.protocol.codec

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FunSuite
import org.junit.Assert._

/**
 *
 */

@RunWith(classOf[JUnitRunner])
class TestStringCodec extends FunSuite {

  val codec = new StringCodec

  test("should decoded with charset") {
    val originalString = "éÉ|akkç"
    val data = originalString.getBytes("UTF-8")

    assertEquals(originalString, codec.decode(data, "UTF-8"))
  }

  test("should encode with charset") {
    val originalString = "éÉ|akkç"
    val data = originalString.getBytes("UTF-8")

    assertArrayEquals(data, codec.encode(originalString, "UTF-8"))
  }

}
