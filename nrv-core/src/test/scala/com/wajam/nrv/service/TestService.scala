package com.wajam.nrv.service

import com.wajam.nrv.cluster.Node
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.scalatest.matchers.ShouldMatchers._
import java.lang.IllegalArgumentException

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
    service.addMember(new ServiceMember(5, new Node("localhost", Map("nrv" -> 12345))))
    service.addMember(new ServiceMember(9, new Node("localhost", Map("nrv" -> 12346))))
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

  test("get member ranges - many members") {
    val member1000 = new ServiceMember(1000, new Node("localhost", Map("nrv" -> 12345)))
    val member2000 = new ServiceMember(2000, new Node("localhost", Map("nrv" -> 12345)))
    service.addMember(member1000)
    service.addMember(member2000)

    val expectedRanges1000 = List(TokenRange(0, 1000),TokenRange(2001, TokenRange.MaxToken))
    service.getMemberTokenRanges(member1000) should be(expectedRanges1000)
    verifyMemberRangeMatchResolve(member1000)

    val expectedRanges2000 = List(TokenRange(1001, 2000))
    service.getMemberTokenRanges(member2000) should be(expectedRanges2000)
    verifyMemberRangeMatchResolve(member2000)
  }

  test("get member ranges - single member") {
    val member1000 = new ServiceMember(1000, new Node("localhost", Map("nrv" -> 12345)))
    service.addMember(member1000)

    val expectedRanges1000 = List(TokenRange(0, TokenRange.MaxToken))
    service.getMemberTokenRanges(member1000) should be(expectedRanges1000)
    verifyMemberRangeMatchResolve(member1000)
  }

  test("get member ranges - first member token is zero") {
    val member0000 = new ServiceMember(0, new Node("localhost", Map("nrv" -> 12345)))
    val member2000 = new ServiceMember(2000, new Node("localhost", Map("nrv" -> 12345)))
    service.addMember(member0000)
    service.addMember(member2000)

    val expectedRanges0000 = List(TokenRange(0, 0),TokenRange(2001, TokenRange.MaxToken))
    service.getMemberTokenRanges(member0000) should be(expectedRanges0000)
    verifyMemberRangeMatchResolve(member0000)

    val expectedRanges2000 = List(TokenRange(1, 2000))
    service.getMemberTokenRanges(member2000) should be(expectedRanges2000)
    verifyMemberRangeMatchResolve(member2000)
  }

  test("get member ranges - last member is max token value") {
    val member1000 = new ServiceMember(1000, new Node("localhost", Map("nrv" -> 12345)))
    val memberMax = new ServiceMember(TokenRange.MaxToken, new Node("localhost", Map("nrv" -> 12345)))
    service.addMember(member1000)
    service.addMember(memberMax)

    val expectedRanges1000 = List(TokenRange(0, 1000))
    service.getMemberTokenRanges(member1000) should be(expectedRanges1000)
    verifyMemberRangeMatchResolve(member1000)

    val expectedRangesMax = List(TokenRange(1001, TokenRange.MaxToken))
    service.getMemberTokenRanges(memberMax) should be(expectedRangesMax)
    verifyMemberRangeMatchResolve(memberMax)
  }

  test("get member ranges - first member is zero and last member is max token value") {
    val member0000 = new ServiceMember(0, new Node("localhost", Map("nrv" -> 12345)))
    val memberMax = new ServiceMember(TokenRange.MaxToken, new Node("localhost", Map("nrv" -> 12345)))
    service.addMember(member0000)
    service.addMember(memberMax)

    val expectedRanges0000 = List(TokenRange(0, 0))
    service.getMemberTokenRanges(member0000) should be(expectedRanges0000)
    verifyMemberRangeMatchResolve(member0000)

    val expectedRangesMax = List(TokenRange(1, TokenRange.MaxToken))
    service.getMemberTokenRanges(memberMax) should be(expectedRangesMax)
    verifyMemberRangeMatchResolve(memberMax)
  }

  test("get member ranges - not a member") {
    service.addMember(new ServiceMember(1000, new Node("localhost", Map("nrv" -> 12345))))
    service.addMember(new ServiceMember(2000, new Node("localhost", Map("nrv" -> 12345))))

    evaluating {
      service.getMemberTokenRanges(new ServiceMember(0, new Node("localhost", Map("nrv" -> 12345))))
    } should produce[IllegalArgumentException]
  }

  def verifyMemberRangeMatchResolve(member: ServiceMember) {
    val ranges = service.getMemberTokenRanges(member)

    for (range <- ranges) {
      service.resolveMembers(range.start, 1)(0) should be(member)
      service.resolveMembers(range.end, 1)(0) should be(member)
    }
  }
}
