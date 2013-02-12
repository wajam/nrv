package com.wajam.nrv.data

import org.scalatest.FunSuite
import org.scalatest.matchers.ShouldMatchers
import com.wajam.nrv.data.MessageMigration._

class TestMessageMigration extends FunSuite with ShouldMatchers {

  test("setValue write both") {
    val map = new collection.mutable.HashMap[String, Any]

    map.setValue("myKey", Seq("1"), 1)

    map.contains("myKey") should be(true)
    map("myKey") should be(1)

    map.contains("myKey_New") should be(true)
    map("myKey_New") should be(Seq("1"))
  }

  test("setNoopValue write once") {
    val map = new collection.mutable.HashMap[String, Any]

    map.setNoopValue("myKey", Seq("1"))

    map.contains("myKey") should be(true)
    map("myKey") should be(Seq("1"))
  }

  test("setEitherStringValue write both") {
    val map = new collection.mutable.HashMap[String, Any]

    map.setBothStringValue("myKey", Seq("1"))

    map.contains("myKey") should be(true)
    map("myKey") should be(Seq("1"))

    map.contains("myKey_New") should be(true)
    map("myKey_New") should be(Seq("1"))
  }

  test("getBothValue gets both") {
    val map = new collection.mutable.HashMap[String, Any]

    map.setValue("myKey", Seq("1"), 1)
    val values = map.getBothValue("myKey")

    values._1 should be(Seq("1"))
    values._2 should be(1)
  }

  test("getValueOrBuildFromString gets Any from string") {
    val map = new collection.mutable.HashMap[String, Any]

    map.setValue("myKey", Seq("1"), 1)

    val value = map.getValueOrBuildFromString("myKey", (v) => (v(0).toInt))

    value should be(1)
  }

  test("getValueOrBuildFromString gets Any from pure value") {
    val map = new collection.mutable.HashMap[String, Any]

    map.setValue("myKey", Seq("1"), 1)

    map -= "myKey_New"

    val value = map.getValueOrBuildFromString("myKey", (v) => (v(0).toInt))

    value should be(1)
  }

  test("containsEither valid when only new") {
    val map = new collection.mutable.HashMap[String, Any]

    map.setValue("myKey", Seq("1"), 1)

    map -= "myKey_New"

    map.containsEither("myKey") should be(true)
  }

  test("containsEither valid when only old") {
    val map = new collection.mutable.HashMap[String, Any]

    map.setValue("myKey", Seq("1"), 1)

    map -= "myKey"

    map.containsEither("myKey") should be(true)
  }

  test("getEitherStringValue valid when only new") {
    val map = new collection.mutable.HashMap[String, Any]

    map += ("myKey_New" -> Seq("1"))

    map.getEitherStringValue("myKey") should be(Seq("1"))
  }

  test("getEitherStringValue valid when only old") {
    val map = new collection.mutable.HashMap[String, Any]

    map += ("myKey" -> Seq("1"))

    map.getEitherStringValue("myKey") should be(Seq("1"))
  }

  test("getFlatStringValue when Seq(String) and new") {
    val map = new collection.mutable.HashMap[String, Any]

    map += ("myKey_New" -> Seq("1"))

    map.getFlatStringValue("myKey") should be(Some("1"))
  }

  test("getFlatStringValue when Seq(String) and old") {
    val map = new collection.mutable.HashMap[String, Any]

    map += ("myKey" -> Seq("1"))

    map.getFlatStringValue("myKey") should be(Some("1"))
  }

  test("getFlatStringValue when string and new") {
    val map = new collection.mutable.HashMap[String, Any]

    map += ("myKey_New" -> "1")

    map.getFlatStringValue("myKey") should be(Some("1"))
  }

  test("getFlatStringValue when string and old") {
    val map = new collection.mutable.HashMap[String, Any]

    map += ("myKey_New" -> "1")

    map.getFlatStringValue("myKey") should be(Some("1"))
  }
}
