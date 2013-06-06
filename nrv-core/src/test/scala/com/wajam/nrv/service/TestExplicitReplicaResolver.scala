package com.wajam.nrv.service

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FunSuite
import org.scalatest.matchers.ShouldMatchers
import com.wajam.nrv.cluster.Node
import com.wajam.nrv.service._

@RunWith(classOf[JUnitRunner])
class TestExplicitReplicaResolver extends FunSuite with ShouldMatchers {
  val node10001 = new Node("localhost", Map("nrv" -> 10001))
  val node10002 = new Node("localhost", Map("nrv" -> 10002))
  val node10003 = new Node("localhost", Map("nrv" -> 10003))
  val node10004 = new Node("localhost", Map("nrv" -> 10004))
  val node10005 = new Node("localhost", Map("nrv" -> 10005))
  val node10006 = new Node("localhost", Map("nrv" -> 10006))

  val service = new Service("testService")
  val memb1000 = service.addMember(new ServiceMember(1000, node10001,MemberStatus.Up))
  val memb2000 = service.addMember(new ServiceMember(2000, node10002,MemberStatus.Up))
  val memb3000 = service.addMember(new ServiceMember(3000, node10003,MemberStatus.Up))
  val memb4000 = service.addMember(new ServiceMember(4000, node10004,MemberStatus.Up))
  val memb5000 = service.addMember(new ServiceMember(5000, node10005,MemberStatus.Up))
  val memb6000 = service.addMember(new ServiceMember(6000, node10006,MemberStatus.Up))

  test("should be able to resolve from the explicit configuration") {
    val ExplicitShardNodeMapping = Map(
      (1000l -> List(node10001,node10005,node10006)),
      (2000l -> List(node10002,node10006,node10001)),
      (3000l -> List(node10003,node10001,node10002)),
      (4000l -> List(node10004,node10002,node10003)),
      (5000l -> List(node10005,node10003,node10004)),
      (6000l -> List(node10006,node10004,node10005))
    )

    val explicitResolver = new ExplicitReplicaResolver(ExplicitShardNodeMapping, new Resolver())
    explicitResolver.resolve(service, 1000).selectedReplicas.map(_.node) should be(List(node10001,node10005,node10006))
    explicitResolver.resolve(service, 2000).selectedReplicas.map(_.node) should be(List(node10002,node10006,node10001))
    explicitResolver.resolve(service, 3000).selectedReplicas.map(_.node) should be(List(node10003,node10001,node10002))
    explicitResolver.resolve(service, 4000).selectedReplicas.map(_.node) should be(List(node10004,node10002,node10003))
    explicitResolver.resolve(service, 5000).selectedReplicas.map(_.node) should be(List(node10005,node10003,node10004))
    explicitResolver.resolve(service, 6000).selectedReplicas.map(_.node) should be(List(node10006,node10004,node10005))
  }
  test("should return the correct master first even if not specified explicitly") {
    val ExplicitShardNodeMapping = Map(
      (1000l -> List(node10005,node10006))
    )

    val explicitResolver = new ExplicitReplicaResolver(ExplicitShardNodeMapping, new Resolver())
    explicitResolver.resolve(service, 1000l).selectedReplicas.map(_.node) should be(List(node10001,node10005,node10006))
  }

  test("should return the correct master first even if no configuration are available") {
    val ExplicitShardNodeMapping:Map[Long,List[Node]] = Map()

    val explicitResolver = new ExplicitReplicaResolver(ExplicitShardNodeMapping, new Resolver())
    explicitResolver.resolve(service, 1000l).selectedReplicas.map(_.node) should be(List(node10001))
    explicitResolver.resolve(service, 2000l).selectedReplicas.map(_.node) should be(List(node10002))
    explicitResolver.resolve(service, 3000l).selectedReplicas.map(_.node) should be(List(node10003))
    explicitResolver.resolve(service, 4000l).selectedReplicas.map(_.node) should be(List(node10004))
    explicitResolver.resolve(service, 5000l).selectedReplicas.map(_.node) should be(List(node10005))
    explicitResolver.resolve(service, 6000l).selectedReplicas.map(_.node) should be(List(node10006))
  }

  test("should return the correct master first even if it is not specified first explicitly") {
    val ExplicitShardNodeMapping = Map(
      (1000l -> List(node10005,node10001,node10006)),
      (2000l -> List(node10001,node10006,node10002))
    )

    val explicitResolver = new ExplicitReplicaResolver(ExplicitShardNodeMapping, new Resolver())

    explicitResolver.resolve(service, 1000l).selectedReplicas.map(_.node) should be(List(node10001,node10005,node10006))
    explicitResolver.resolve(service, 2000l).selectedReplicas.map(_.node) should be(List(node10002,node10001,node10006))
  }
}
