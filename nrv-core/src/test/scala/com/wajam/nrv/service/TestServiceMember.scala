package com.wajam.nrv.service

import org.scalatest.FunSuite
import org.scalatest.matchers.ShouldMatchers._
import com.wajam.nrv.cluster.Node
import com.wajam.nrv.utils.InetUtils

class TestServiceMember extends FunSuite {

  test("service member should have a toString and be buildable with fromString") {
    val sm1 = new ServiceMember(100, new Node("127.0.0.1", Map("nrv" -> 123)))
    val expected = "100:%s:nrv=123".format(InetUtils.firstInetAddress.get.getHostName)
    sm1.toString should be(expected)
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
