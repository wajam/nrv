package com.wajam.nrv.service

import com.wajam.nrv.cluster.Node
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite}

@RunWith(classOf[JUnitRunner])
class TestService extends FunSuite with BeforeAndAfter {
  var service: Service = _

  before {
    service = new Service("test")
  }

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

  test("add many actions and make sure they are in reverse order") {
    val action1 = new Action("/test1", (req) => Unit)
    val action2 = new Action("/test2", (req) => Unit)
    service.registerActions(List(action1, action2))
    assert(service.actions.size == 2)
    assert(service.actions(0) == action2)
    assert(service.actions(1) == action1)
  }
}
