package com.wajam.nrv.service

import org.scalatest.FunSuite
import com.wajam.nrv.cluster.Node
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TestService extends FunSuite {
  val service = new Service("test")

  test("add action") {
    service.bind("/test1", new Action((req) => Unit))
    service.bind("/test2", new Action((req) => Unit))
    assert(service.actions.size == 2)
  }

  test("add member") {
    service.add(5, new Node("localhost", Map("nrv" -> 12345)))
    service.add(9, new Node("localhost", Map("nrv" -> 12346)))
    assert(service.size == 2)
  }

  test("member resolve") {
    var endpoints = service.resolve(8, 1)
    assert(endpoints.size == 1)
    assert(endpoints(0).token == 9)

    endpoints = service.resolve(10, 2)
    assert(endpoints.size == 2)
    assert(endpoints(0).token == 5)
    assert(endpoints(1).token == 9)
  }
}
