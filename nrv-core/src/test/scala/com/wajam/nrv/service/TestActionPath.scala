package com.wajam.nrv.service

import org.scalatest.FunSuite
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import com.wajam.nrv.InvalidParameter

@RunWith(classOf[JUnitRunner])
class TestActionPath extends FunSuite {

  test("path parsing") {
    val url1 = new ActionPath("/test/:var/resource")
    var (matches1, data1) = url1.matchesPath("/test/value/resource")
    assert(matches1)
    assert(data1.get("var") == Some("value"))

    var (matches2, _) = url1.matchesPath("/test/value")
    assert(!matches2)

    var (matches3, _) = url1.matchesPath("")
    assert(!matches2)

    val url2 = new ActionPath("/")
    var (matches4, data4) = url2.matchesPath("/")
    assert(matches4)
    assert(data4.size == 0)
  }

  test("path bulding") {
    val url1 = new ActionPath("/test/:var/resource")
    assert(url1.buildPath(Map("var" -> "value1")) == "/test/value1/resource")

    val url2 = new ActionPath("/test/resource")
    assert(url2.buildPath() == "/test/resource")

    val url3 = new ActionPath("/")
    assert(url3.buildPath() == "/")

    intercept[InvalidParameter] {
      val url4 = new ActionPath("/test/:var/resource")
      url4.buildPath()
    }
  }
}
