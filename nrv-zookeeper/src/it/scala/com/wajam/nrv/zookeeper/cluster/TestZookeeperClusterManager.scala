package com.wajam.nrv.zookeeper.cluster

import com.yammer.metrics.Metrics
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.scalatest.matchers.ShouldMatchers
import com.wajam.nrv.cluster.{LocalNode, Node, Cluster}
import com.wajam.nrv.service
import com.wajam.nrv.service._
import com.wajam.nrv.zookeeper.{ZookeeperTestHelpers, ZookeeperClient}
import com.wajam.nrv.zookeeper.ZookeeperClient._

class TestZookeeperClusterManager extends FunSuite with BeforeAndAfter with ShouldMatchers {

  var clusters = List[TestCluster]()

  before {
    // cleanup before
    val zk = new ZookeeperClient("127.0.0.1")
    try {
      zk.deleteRecursive("/tests/clustermanager")
    } catch {
      case e: Exception =>
    }
    zk.ensureAllExists("/tests/clustermanager", "")
    zk.close()
  }

  after {
    clusters.foreach(_.stop())
    clusters = List[TestCluster]()
  }

  class TestCluster(id: Int) extends ZookeeperTestHelpers {
    val localNode = new LocalNode("127.0.0.1", Map("nrv" -> (10010 + id)))

    // create a zookeeper chrooted on /tests/clustermanager
    val zk = new ZookeeperClient("127.0.0.1/tests/clustermanager")
    val clusterManager = new ZookeeperClusterManager(zk)
    val cluster = new Cluster(localNode, clusterManager)
    val service1 = new Service("service1")
    val service2 = new Service("service2")
    cluster.registerService(service1)
    cluster.registerService(service2)

    val n1 = new Node("127.0.0.1", Map("nrv" -> (10011)))
    val n2 = new Node("127.0.0.1", Map("nrv" -> (10012)))
    val n3 = new Node("127.0.0.1", Map("nrv" -> (10013)))
    val service1_member5 = new ServiceMember(5, n1)
    val service1_member10 = new ServiceMember(10, n2)
    val service1_member20 = new ServiceMember(20, n3)
    val service2_member7 = new ServiceMember(7, n1)
    val service2_member16 = new ServiceMember(16, n2)
    val service2_member32 = new ServiceMember(32, n3)

    zkCreateService(service1)
    zkCreateService(service2)

    zkCreateServiceMember(service1, service1_member5)
    zkCreateServiceMember(service1, service1_member10)
    zkCreateServiceMember(service1, service1_member20)

    zkCreateServiceMember(service2, service2_member7)
    zkCreateServiceMember(service2, service2_member16)
    zkCreateServiceMember(service2, service2_member32)

    def start() = {
      cluster.start()
      this
    }

    def stop(shudownTimeOutInMs: Long = 5000L) = {
      cluster.stop(shudownTimeOutInMs)
      zk.close()
      this
    }

    def allMembers = for ((_, service) <- cluster.services; member <- service.members) yield member
  }

  def createCluster(id: Int): TestCluster = {
    val cluster = new TestCluster(id)
    clusters :+= cluster
    cluster
  }

  def waitForCondition[T](block: => T, condition: (T) => Boolean, sleepTimeInMs: Long = 250, timeoutInMs: Long = 15000) {
    val startTime = System.currentTimeMillis()
    while (!condition(block)) {
      if ((System.currentTimeMillis() - startTime) > timeoutInMs) {
        throw new RuntimeException("Timeout waiting for condition.")
      }
      Thread.sleep(sleepTimeInMs)
    }
  }

  test("a cluster should be initialised with service members from zookeeper") {
    val cluster = createCluster(1).start()

    assert(cluster.allMembers.size == 6)
    assert(cluster.service1.getMemberAtToken(5).isDefined)
    assert(cluster.service1.getMemberAtToken(10).isDefined)
    assert(cluster.service2.getMemberAtToken(7).isDefined)
    assert(cluster.service2.getMemberAtToken(16).isDefined)
  }

  test("a service member with no votes should be down") {
    val cluster = createCluster(1).start()

    cluster.allMembers.foreach(member => assert(member.status == MemberStatus.Down))
  }

  test("a service member with one vote up from himself should be up") {
    val cluster = createCluster(1)
    cluster.zkCastVote(cluster.service2, cluster.service2_member7, cluster.service2_member7, MemberStatus.Up)
    cluster.start()

    assert(cluster.service2.getMemberAtToken(7).get.status == MemberStatus.Up)
  }

  test("a service member for which votes are deleted or added should change status") {
    val cluster = createCluster(1)
    cluster.zkCastVote(cluster.service2, cluster.service2_member16, cluster.service2_member16, MemberStatus.Up)
    cluster.zkCastVote(cluster.service2, cluster.service2_member32, cluster.service2_member32, MemberStatus.Up)
    cluster.start()

    cluster.zkDeleteVote(cluster.service2, cluster.service2_member16, cluster.service2_member16)

    waitForCondition[MemberStatus]({
      cluster.service2.getMemberAtToken(16).get.status
    }, _ == MemberStatus.Down)

    cluster.zkCastVote(cluster.service2, cluster.service2_member16, cluster.service2_member16, MemberStatus.Up)

    waitForCondition[MemberStatus]({
      val status = cluster.service2.getMemberAtToken(16).get.status
      status
    }, _ == MemberStatus.Up)

    cluster.zkDeleteVote(cluster.service2, cluster.service2_member16, cluster.service2_member16)

    waitForCondition[MemberStatus]({
      cluster.service2.getMemberAtToken(16).get.status
    }, _ == MemberStatus.Down)
  }

  test("adding or removing a new service member should add or remove it from existing service") {
    val cluster1 = createCluster(1).start()

    val newMember = new ServiceMember(72, cluster1.localNode)
    cluster1.zkCreateServiceMember(cluster1.service1, newMember)

    // wait to be added
    waitForCondition[Option[ServiceMember]]({
      cluster1.service1.getMemberAtToken(72)
    }, _ == Some(newMember))

    // wait to come up
    waitForCondition[MemberStatus]({
      cluster1.service1.getMemberAtToken(72).get.status
    }, _ == MemberStatus.Up)

    cluster1.zkDeleteServiceMember(cluster1.service1, newMember)

    // wait to be removed
    waitForCondition[Option[ServiceMember]]({
      cluster1.service1.getMemberAtToken(72)
    }, _ == None)
  }

  test("a service member should join and become up after joining") {
    val cluster1 = createCluster(1).start()
    val cluster2 = createCluster(2).start()

    // wait to come up in both cluster
    waitForCondition[MemberStatus]({
      cluster1.service2.getMemberAtToken(7).get.status
    }, _ == MemberStatus.Up)

    waitForCondition[MemberStatus]({
      cluster2.service2.getMemberAtToken(7).get.status
    }, _ == MemberStatus.Up)
  }

  test("a service member migration to another node should be kind of seamless") {
    val cluster1 = createCluster(1).start()
    val cluster2 = createCluster(2).start()

    val newMember1 = new ServiceMember(72, cluster1.localNode)
    cluster1.zkCreateServiceMember(cluster1.service1, newMember1)

    // wait to be added
    waitForCondition[Option[ServiceMember]]({
      cluster1.service1.getMemberAtToken(72)
    }, _ == Some(newMember1))

    // wait to come up
    waitForCondition[MemberStatus]({
      cluster1.service1.getMemberAtToken(72).get.status
    }, _ == MemberStatus.Up)

    // Change service member from node cluster1 to cluster2
    val newMember2 = new ServiceMember(72, cluster2.localNode)
    cluster1.zkCreateServiceMember(cluster1.service1, newMember2)

    // wait to come up on cluster2
    waitForCondition[MemberStatus]({
      cluster2.service1.getMemberAtToken(72).get.status
    }, _ == MemberStatus.Up)

    // wait to be removed on cluster1 and moved to cluster2
    waitForCondition[Option[ServiceMember]]({
      cluster1.service1.getMemberAtToken(72)
    }, _ == Some(newMember2))
  }

  test("members of a crashed cluster should become down and come back up when rejoining") {
    val cluster1 = createCluster(1).start()
    var cluster2 = createCluster(2).start()

    // wait to come up
    waitForCondition[MemberStatus]({
      cluster1.service2.getMemberAtToken(16).get.status
    }, _ == MemberStatus.Up)

    // stop cluster, wait to come down
    cluster2.stop()
    waitForCondition[MemberStatus]({
      cluster1.service2.getMemberAtToken(16).get.status
    }, _ == MemberStatus.Down)

    // restart cluster, wait to come up
    cluster2 = createCluster(2).start()
    waitForCondition[MemberStatus]({
      cluster1.service2.getMemberAtToken(16).get.status
    }, _ == MemberStatus.Up)
  }

  test("zookeeper connection loss should bring members down and they should come back once zookeeper reconnect") {
    val cluster1 = createCluster(1).start()
    var cluster2 = createCluster(2).start()

    // wait to come up
    waitForCondition[MemberStatus]({
      cluster1.service2.getMemberAtToken(16).get.status
    }, _ == MemberStatus.Up)

    // stop cluster, wait to come down
    cluster2.zk.close()
    waitForCondition[MemberStatus]({
      cluster1.service2.getMemberAtToken(16).get.status
    }, _ == MemberStatus.Down)

    // restart cluster, wait to come up
    cluster2.zk.connect()
    waitForCondition[MemberStatus]({
      cluster1.service2.getMemberAtToken(16).get.status
    }, _ == MemberStatus.Up)
  }

  test("sabotaged service member should prevent cluster from starting") {
    val cluster = createCluster(1)

    // Sabotage service member by setting an empty service member data
    val path = ZookeeperClusterManager.zkMemberPath(cluster.service1.name, cluster.service1_member10.token)
    cluster.zk.set(path, "")

    evaluating {
      cluster.start()
    } should produce[Exception]

    cluster.stop()
  }

  test("sabotaged service member of a started cluster should update the sync error counter") {
    val syncErrorCounter = Metrics.defaultRegistry().newCounter(classOf[ZookeeperClusterManager], "sync-error-count", "all")

    val cluster1 = createCluster(1).start()
    val cluster2 = createCluster(2).start()
    val cluster3 = createCluster(3).start()
    syncErrorCounter.count() should be(0)

    // wait to come up
    waitForCondition[Iterable[MemberStatus]]({
      cluster1.allMembers.map(_.status)
    }, _.forall(_ == MemberStatus.Up))

    // sabotage service member and wait for sync error counter to increment
    val path = ZookeeperClusterManager.zkMemberPath(cluster1.service2.name, cluster1.service2_member16.token)
    cluster1.zk.set(path, "")
    waitForCondition[Long]({
      syncErrorCounter.count()
    }, _ > 0)

    // repair service member and wait sync error counter reset
    cluster1.zkCreateServiceMember(cluster1.service2, cluster1.service2_member16)
    waitForCondition[Long]({
      syncErrorCounter.count()
    }, _ == 0)
  }

  test("when stopping a cluster, ServiceMembers should change like this: Up -> Leaving -> Down, and the Leaving -> Down change should be done according to votes") {
    import com.wajam.nrv.service.{StatusTransitionAttemptEvent, MemberStatus}
    import com.wajam.commons.Event
    object VetoVoter {
      private var callCountDown = 3
      private var callCounter = 0
      def getCallCountDown = callCountDown
      def getCallCounter = callCounter

      def ApplyVetoMultipleTimes(event: Event) {
        synchronized {
          callCounter+=1
          event match {
            case e: StatusTransitionAttemptEvent if e.to == MemberStatus.Down && callCountDown == 0 => {
              e.vote(pass = true)
              e.from should be (service.MemberStatus.Leaving)
            }
            case e: StatusTransitionAttemptEvent if e.to == MemberStatus.Down && callCountDown > 0 => {
              callCountDown -= 1
              e.vote(pass = false)
              e.from should be (service.MemberStatus.Leaving)
            }
            case e: Event =>
          }
        }
      }
    }

    val cluster = createCluster(1).start()
    cluster.service1.addObserver(VetoVoter.ApplyVetoMultipleTimes)

    cluster.stop(5000L)

    //all members should be down
    cluster.service1.members.foreach(_.status should be (MemberStatus.Down))
    cluster.service2.members.foreach(_.status should be (MemberStatus.Down))
    //the Vetovoter should have applied his veto 3 times, and should have been
    VetoVoter.getCallCountDown should be(0)
    //The Vetovoter should have been called 3 times (3 fail attempts) + once for each service1 member
    VetoVoter.getCallCounter should be(3 + cluster.service1.members.size)
  }

}
