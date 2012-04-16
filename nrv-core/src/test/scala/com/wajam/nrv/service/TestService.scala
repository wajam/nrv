package com.wajam.nrv.service

import org.scalatest.FunSuite
import com.wajam.nrv.cluster.Node
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TestService extends FunSuite {
  val service = new Service("test")

  test("add action") {
    service.registerAction(new Action("/test1", (req) => Unit))
    service.registerAction(new Action("/test2", (req) => Unit))
    assert(service.actions.size == 2)
  }

  test("add member") {
    service.addMember(5, new Node("localhost", Map("nrv" -> 12345)))
    service.addMember(9, new Node("localhost", Map("nrv" -> 12346)))
    assert(service.membersCount == 2)
  }

  test("member resolve") {
    var endpoints = service.resolveMembers(8, 1)
    assert(endpoints.size == 1)
    assert(endpoints(0).token == 9)

    endpoints = service.resolveMembers(10, 2)
    assert(endpoints.size == 2)
    assert(endpoints(0).token == 5)
    assert(endpoints(1).token == 9)
  }
}
