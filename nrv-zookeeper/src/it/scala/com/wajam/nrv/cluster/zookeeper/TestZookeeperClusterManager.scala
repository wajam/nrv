package com.wajam.nrv.cluster.zookeeper

import org.scalatest.{BeforeAndAfter, FunSuite}
import com.wajam.nrv.cluster.{ServiceMemberVote, Node, Cluster}
import com.wajam.nrv.cluster.zookeeper.ZookeeperClient._
import com.wajam.nrv.service.{MemberStatus, ServiceMember, Service}
import org.apache.zookeeper.CreateMode

class TestZookeeperClusterManager extends FunSuite with BeforeAndAfter {

  val localNode = new Node("127.0.0.1", Map("nrv" -> 1234))
  var cluster: Cluster = _
  var clusterManager: ZookeeperClusterManager = _
  var service1: Service = _
  var service2: Service = _
  var zk: ZookeeperClient = _

  val n1 = new Node("10.0.0.1", Map("nrv" -> 10001))
  val n2 = new Node("10.0.0.2", Map("nrv" -> 10001))
  val service1_member5 = new ServiceMember(5, n1)
  val service1_member10 = new ServiceMember(10, n2)
  val service2_member7 = new ServiceMember(7, n1)
  val service2_member16 = new ServiceMember(16, n2)


  before {
    // cleanup before
    zk = new ZookeeperClient("127.0.0.1")
    try {
      zk.deleteRecursive("/tests/clustermanager")
    } catch {
      case e: Exception =>
    }
    zk.ensureExists("/tests", "")
    zk.ensureExists("/tests/clustermanager", "")
    zk.close()


    // create a zookeeper chrooted on /tests/clustermanager
    zk = new ZookeeperClient("127.0.0.1/tests/clustermanager")
    clusterManager = new ZookeeperClusterManager(zk)
    cluster = new Cluster(localNode, clusterManager)
    service1 = new Service("service1")
    service2 = new Service("service2")
    cluster.registerService(service1)
    cluster.registerService(service2)
  }

  after {
    cluster.stop()
  }

  def zkCreateService(service: Service) {
    val path = ZookeeperClusterManager.zkServicePath(service.name)
    zk.ensureExists(path, service.name, CreateMode.PERSISTENT)
  }

  def zkCreateServiceMember(service: Service, serviceMember: ServiceMember) {
    val path = ZookeeperClusterManager.zkMemberPath(service.name, serviceMember.token)
    val exists = zk.ensureExists(path, serviceMember.toString, CreateMode.PERSISTENT)

    // if node existed, overwrite
    if (exists)
      zk.set(path, serviceMember.toString)
  }

  def zkCastVote(service: Service, candidateMember: ServiceMember, voterMember: ServiceMember, votedStatus: MemberStatus) {
    val vote = new ServiceMemberVote(candidateMember, voterMember, votedStatus)
    val path = ZookeeperClusterManager.zkMemberVotePath(service.name, candidateMember.token, voterMember.token)
    val exists = zk.ensureExists(path, vote.toString, CreateMode.PERSISTENT)

    // if node existed, overwrite
    if (exists)
      zk.set(path, vote.toString)
  }

  def zkDeleteVote(service: Service, candidateMember: ServiceMember, voterMember: ServiceMember) {
    val path = ZookeeperClusterManager.zkMemberVotePath(service.name, candidateMember.token, voterMember.token)
    zk.delete(path)
  }

  def createBaseCluster() {
    zkCreateService(service1)
    zkCreateService(service2)
    zkCreateServiceMember(service1, service1_member5)
    zkCreateServiceMember(service1, service1_member10)

    zkCreateServiceMember(service2, service2_member7)
    zkCreateServiceMember(service2, service2_member16)
  }

  protected def allMembers = for ((_, service) <- cluster.services; member <- service.members) yield member

  def waitForCondition[T](block: => T, condition: (T) => Boolean, sleepTimeInMs: Long = 250, timeoutInMs: Long = 10000) {
    val startTime = System.currentTimeMillis()
    while (!condition(block)) {
      if ((System.currentTimeMillis() - startTime) > timeoutInMs) {
        throw new RuntimeException("Timeout waiting for condition.")
      }
      Thread.sleep(sleepTimeInMs)
    }
  }

  test("a cluster should be initialised with service members from zookeeper") {
    createBaseCluster()
    cluster.start()

    assert(allMembers.size == 4)
    assert(service1.getMemberAtToken(5).isDefined)
    assert(service1.getMemberAtToken(10).isDefined)
    assert(service2.getMemberAtToken(7).isDefined)
    assert(service2.getMemberAtToken(16).isDefined)
  }

  test("a service member with no votes should be down") {
    createBaseCluster()
    cluster.start()

    allMembers.foreach(member => assert(member.status == MemberStatus.Down))
  }

  test("(temp) a service member with one vote up from himself should be up") {
    createBaseCluster()
    zkCastVote(service2, service2_member7, service2_member7, MemberStatus.Up)
    cluster.start()

    assert(service2.getMemberAtToken(7).get.status == MemberStatus.Up)
  }

  test("(temp) a service member for which votes are deleted or added should change status") {
    createBaseCluster()
    zkCastVote(service2, service2_member7, service2_member7, MemberStatus.Up)
    zkCastVote(service2, service2_member16, service2_member16, MemberStatus.Up)
    cluster.start()

    zkDeleteVote(service2, service2_member7, service2_member7)

    waitForCondition[MemberStatus]({
      service2.getMemberAtToken(7).get.status
    }, _ == MemberStatus.Down)

    zkCastVote(service2, service2_member7, service2_member7, MemberStatus.Up)

    waitForCondition[MemberStatus]({
      val status = service2.getMemberAtToken(7).get.status
      println(status)
      status
    }, _ == MemberStatus.Up)

    zkDeleteVote(service2, service2_member7, service2_member7)

    waitForCondition[MemberStatus]({
      service2.getMemberAtToken(7).get.status
    }, _ == MemberStatus.Down)
  }
}
