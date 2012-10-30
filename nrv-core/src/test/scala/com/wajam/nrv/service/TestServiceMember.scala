package com.wajam.nrv.service

import org.scalatest.FunSuite
import com.wajam.nrv.cluster.Node

class TestServiceMember extends FunSuite {

  test("service member should have a toString and be buildable with fromString") {
    val sm1 = new ServiceMember(100, new Node("127.0.0.1", Map("nrv" -> 123)))
    assert(sm1.toString == "100:localhost:nrv=123", sm1.toString)
    val sm2 = ServiceMember.fromString(sm1.toString)
    assert(sm1.token == sm2.token)
    assert(sm1.node == sm2.node)
  }

  test("service member status should have a toString and fromString") {
    val statuses = Seq(MemberStatus.Down, MemberStatus.Joining, MemberStatus.Up)
    for (status <- statuses) {
      assert(MemberStatus.fromString(status.toString) == status)
    }
  }

  test("service member with same token and node should be equals") {
    val sm1 = new ServiceMember(100, new Node("127.0.0.1", Map("nrv" -> 123)))
    val sm2 = new ServiceMember(100, new Node("127.0.0.1", Map("nrv" -> 123)))
    assert(sm1 == sm2)
  }

}
