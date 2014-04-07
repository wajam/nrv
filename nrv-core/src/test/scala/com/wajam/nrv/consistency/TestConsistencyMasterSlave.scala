package com.wajam.nrv.consistency

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{Matchers, FlatSpec}
import com.wajam.nrv.service.{ExplicitReplicaResolver, Resolver, MemberStatus, ServiceMember}
import com.wajam.nrv.cluster.{Node, LocalNode, Cluster}
import com.wajam.nrv.utils.timestamp.TimestampGenerator
import org.scalatest.concurrent.{IntegrationPatience, Eventually}
import java.io.File
import java.nio.file.Files
import scala.concurrent.{ExecutionContext, Await}
import scala.concurrent.duration._
import java.util.concurrent.TimeUnit
import com.wajam.nrv.UnavailableException
import org.apache.commons.io.FileUtils

@RunWith(classOf[JUnitRunner])
class TestConsistencyMasterSlave extends FlatSpec with Matchers with Eventually with IntegrationPatience {

  trait ClusterFixture {

    case class FixtureParam(logDir: File) {

      private var clusterNodes: List[ConsistentCluster] = Nil

      val node5555 = Node.fromString("localhost:nrv=5555")
      val node6666 = Node.fromString("localhost:nrv=6666")
      val node7777 = Node.fromString("localhost:nrv=7777")
      val node8888 = Node.fromString("localhost:nrv=8888")

      val member1073741823_5555 = new ServiceMember(1073741823L, node5555)
      val member2147483646_6666 = new ServiceMember(2147483646L, node6666)
      val member3221225469_7777 = new ServiceMember(3221225469L, node7777)
      val member4294967292_8888 = new ServiceMember(4294967292L, node8888)

      // Uses only two service member (i.e. shard) by default on nodes 6666 and 8888
      val serviceCache = new ServiceMemberClusterStorage("dummy-consistent-store")
      serviceCache.addMember(member2147483646_6666)
      serviceCache.addMember(member4294967292_8888)

      val consistencyPersistence = new DummyConsistencyPersistence(serviceCache,
        explicitReplicasMapping = Map(
          member1073741823_5555.token -> List(node5555, node6666),
          member2147483646_6666.token -> List(node5555, node6666),
          member3221225469_7777.token -> List(node7777, node8888),
          member4294967292_8888.token -> List(node7777, node8888)
        ))

      def createClusterNode(localNrvPort: Int, naturalRingReplicas: Boolean = true): ConsistentCluster = {
        val nodeLogDir = new File(logDir, localNrvPort.toString)
        Files.createDirectories(nodeLogDir.toPath)
        val clusterNode = new ConsistentCluster(serviceCache, consistencyPersistence, timestampGenerator,
          localNrvPort, nodeLogDir.getCanonicalPath, naturalRingReplicas)
        clusterNodes = clusterNode :: clusterNodes
        clusterNode.start()
        clusterNode
      }

      def stop(): Unit = {
        clusterNodes.foreach(_.stop())
      }
    }

    val timestampGenerator = new DummyTimestampGenerator

    val awaitDuration = Duration(3L, TimeUnit.SECONDS)

    implicit val ec = ExecutionContext.Implicits.global

    def withFixture(test: (FixtureParam) => Unit) {
      val logDir: File = Files.createTempDirectory(s"TestConsistencyMasterSlave-").toFile
      val fixture = FixtureParam(logDir)
      try {
        test(fixture)
      } finally {
        fixture.stop()
        FileUtils.deleteDirectory(logDir)
      }
    }

  }

  class ConsistentCluster(serviceCache: ServiceMemberClusterStorage,
                          consistencyPersistence: ConsistencyPersistence,
                          timestampGenerator: TimestampGenerator,
                          localNrvPort: Int,
                          logDir: String,
                          naturalRingReplicas: Boolean) {

    val service = new DummyConsistentStoreService(serviceCache.serviceName, replicasCount = 2)

    val clusterManager = new DummyDynamicClusterManager(Map(serviceCache.serviceName -> serviceCache))

    val localNode = new LocalNode("localhost", Map("nrv" -> localNrvPort))

    val cluster = new Cluster(localNode, clusterManager)
    cluster.registerService(service)

    val consistency = new ConsistencyMasterSlave(timestampGenerator, consistencyPersistence, logDir,
      txLogEnabled = true, replicationResolver = replicationResolver)
    service.applySupport(consistency = Some(consistency))
    consistency.bindService(service)

    def getMemberByToken(token: Long): ServiceMember = service.members.find(m => m.token == token).get

    def getMemberByPort(nrvPort: Int): ServiceMember = service.members.find(m => m.node.ports("nrv") == nrvPort).get

    def getLocalMember = getMemberByPort(localNrvPort)

    def getLocalMemberConsistencyState(token: Long): Option[MemberConsistencyState] = {
      consistency.localMembersStates.collectFirst { case (m, s) if m.token == token => s }
    }

    def getHostingNodePortForValue(value: String): Int = service.resolveMembers(Resolver.hashData(value), 1).head.node.ports("nrv")

    def groupValuesByHostingNodePort(values: List[String]): Map[Int, List[String]] = {
      val grouped: Map[Int, List[(Int, String)]] = values.map(v => (getHostingNodePortForValue(v), v)).groupBy(_._1)
      grouped.map { case (port, tupledValues) => (port, tupledValues.map(_._2)) }
    }

    def start(): Unit = {
      cluster.start()
    }

    def stop(): Unit = {
      cluster.stop(5000)
    }

    private def replicationResolver: Option[Resolver] = {
      if (naturalRingReplicas) {
        None
      } else {
        Some(new ExplicitReplicaResolver(consistencyPersistence.explicitReplicasMapping, service.resolver))
      }
    }

  }

  "ConsistencyMasterSlave" should "replicate master values to slave" in new ClusterFixture {
    withFixture { f =>
      // Start first custer node
      val clusterNode6666 = f.createClusterNode(6666, naturalRingReplicas = true)
      eventually { clusterNode6666.getLocalMember.status should be(MemberStatus.Up) }

      val values = clusterNode6666.groupValuesByHostingNodePort(List("v1", "v2", "v3", "v4", "v5", "v6"))
      values(6666).size should be > 1

      // Add a value to first cluster node
      val headValue = values(6666).head
      val headKey = Await.result(clusterNode6666.service.addRemoteValue(headValue), awaitDuration)
      val actual = Await.result(clusterNode6666.service.getRemoteValue(headKey), awaitDuration)
      actual should be(headValue)

      // Start second cluster node
      val clusterNode8888 = f.createClusterNode(8888, naturalRingReplicas = true)
      eventually { clusterNode8888.getLocalMember.status should be(MemberStatus.Up) }

      // Wait until the value is replicated to the shard slave
      eventually { clusterNode8888.service.getLocalValue(headKey.timestamp) should be(Some(headValue)) }

      // Add remaining values to first cluster node
      val tailPairs = values(6666).tail.map(v => {
        val k = Await.result(clusterNode6666.service.addRemoteValue(v), awaitDuration)
        (k, v)
      })

      // Wait until all values are replicated
      eventually {
        tailPairs.foreach { case (k, v) => clusterNode8888.service.getLocalValue(k.timestamp) should be(Some(v)) }
      }
    }
  }

  it should "allow read from slave when master is down" in new ClusterFixture {
    withFixture { f =>
    // Start first custer node
      val clusterNode6666 = f.createClusterNode(6666, naturalRingReplicas = true)
      eventually { clusterNode6666.getLocalMember.status should be(MemberStatus.Up) }

      val values = clusterNode6666.groupValuesByHostingNodePort(List("v1", "v2", "v3", "v4", "v5", "v6"))
      values(6666).size should be > 1

      // Add a value to first cluster node
      val headValue = values(6666).head
      val headKey = Await.result(clusterNode6666.service.addRemoteValue(headValue), awaitDuration)
      val actual = Await.result(clusterNode6666.service.getRemoteValue(headKey), awaitDuration)
      actual should be(headValue)

      // Start second cluster node
      val clusterNode8888 = f.createClusterNode(8888, naturalRingReplicas = true)
      eventually { clusterNode8888.getLocalMember.status should be(MemberStatus.Up) }

      // Wait until the value is replicated to the shard slave
      eventually { clusterNode8888.service.getLocalValue(headKey.timestamp) should be(Some(headValue)) }

      // Stop first cluster node
      clusterNode6666.stop()
      eventually { clusterNode8888.getMemberByPort(6666).status should be(MemberStatus.Down) }

      // Get the value, it should be served by the slave
      val actual2 = Await.result(clusterNode8888.service.getRemoteValue(headKey), awaitDuration)
      actual2 should be(headValue)
    }
  }

  it should "NOT allow read from slave when master is down and lag too large" in new ClusterFixture {
    withFixture { f =>
      // Start first custer node
      val clusterNode6666 = f.createClusterNode(6666, naturalRingReplicas = true)
      eventually { clusterNode6666.getLocalMember.status should be(MemberStatus.Up) }

      val values = clusterNode6666.groupValuesByHostingNodePort(List("v1", "v2", "v3", "v4", "v5", "v6"))
      values(6666).size should be > 1

      // Add a value to first cluster node
      val headValue = values(6666).head
      val headKey = Await.result(clusterNode6666.service.addRemoteValue(headValue), awaitDuration)
      val actual = Await.result(clusterNode6666.service.getRemoteValue(headKey), awaitDuration)
      actual should be(headValue)

      // Start second cluster node
      val clusterNode8888 = f.createClusterNode(8888, naturalRingReplicas = true)
      eventually { clusterNode8888.getLocalMember.status should be(MemberStatus.Up) }

      // Wait until the value is replicated to the shard slave
      eventually { clusterNode8888.service.getLocalValue(headKey.timestamp) should be(Some(headValue)) }

      // Stop first cluster node
      clusterNode6666.stop()
      eventually { clusterNode8888.getMemberByPort(6666).status should be(MemberStatus.Down) }

      // Artificially increase the replication lag before trying to get the value
      val masterToken = clusterNode6666.getLocalMember.token
      f.consistencyPersistence.updateReplicationLagSeconds(masterToken, clusterNode8888.localNode, 2)
      evaluating {
        Await.result(clusterNode8888.service.getRemoteValue(headKey), awaitDuration)
      } should produce[UnavailableException]
    }
  }

  it should "NOT allow write on slave when master is down" in new ClusterFixture {
    withFixture { f =>
      // Start first custer node
      val clusterNode6666 = f.createClusterNode(6666, naturalRingReplicas = true)
      eventually { clusterNode6666.getLocalMember.status should be(MemberStatus.Up) }

      val values = clusterNode6666.groupValuesByHostingNodePort(List("v1", "v2", "v3", "v4", "v5", "v6"))
      values(6666).size should be > 1

      // Add a value to first cluster node
      val headValue = values(6666).head
      val headKey = Await.result(clusterNode6666.service.addRemoteValue(headValue), awaitDuration)
      val actual = Await.result(clusterNode6666.service.getRemoteValue(headKey), awaitDuration)
      actual should be(headValue)

      // Start second cluster node
      val clusterNode8888 = f.createClusterNode(8888, naturalRingReplicas = true)
      eventually { clusterNode8888.getLocalMember.status should be(MemberStatus.Up) }

      // Wait until the value is replicated to the shard slave
      eventually { clusterNode8888.service.getLocalValue(headKey.timestamp) should be(Some(headValue)) }

      // Stop first cluster node
      clusterNode6666.stop()
      eventually { clusterNode8888.getMemberByPort(6666).status should be(MemberStatus.Down) }

      // Try to write a value to the shard with master down
      evaluating {
        val value2 = values(6666).tail.head
        Await.result(clusterNode8888.service.addRemoteValue(value2), awaitDuration)
      } should produce[UnavailableException]
    }
  }

  it should "recover from consistency error when store consistency is restored" in new ClusterFixture {
    withFixture { f =>
      // Start the first cluster node
      val clusterNode6666 = f.createClusterNode(6666)
      eventually { clusterNode6666.getLocalMember.status should be(MemberStatus.Up) }

      val values = clusterNode6666.groupValuesByHostingNodePort(List("v1", "v2", "v3", "v4", "v5", "v6"))
      values(6666).size should be > 1

      // Add a value to first cluster node and stop it
      val headValue = values(6666).head
      val headKey = Await.result(clusterNode6666.service.addRemoteValue(headValue), awaitDuration)
      val actual = Await.result(clusterNode6666.service.getRemoteValue(headKey), awaitDuration)
      actual should be(headValue)

      clusterNode6666.stop()
      eventually { clusterNode6666.getMemberByPort(6666).status should be(MemberStatus.Down) }

      // Remove the value directly from the local consistent store without going thru NRV service messaging.
      // This break the consistency between the store and the transaction logs.
      // This prevents the service status to goes Up when the cluster node is restarted
      val removedValue = clusterNode6666.service.removeLocal(headKey.timestamp).get
      val clusterNode6666_2 = f.createClusterNode(6666) // TODO: restart the existing node instance
      eventually {
        val token = clusterNode6666_2.getLocalMember.token
        clusterNode6666_2.getLocalMemberConsistencyState(token) should be(Some(MemberConsistencyState.Error))
      }

      // Restore the deleted value. The service consistency should recover and the service status goes Up
      clusterNode6666_2.service.addLocal(removedValue)
      eventually { clusterNode6666_2.getLocalMember.status should be(MemberStatus.Up) }
    }
  }

  it should "handle service member mastership migration" in new ClusterFixture {
    withFixture { f =>
    // Start the first cluster node
      val clusterNode6666 = f.createClusterNode(6666, naturalRingReplicas = true)
      eventually { clusterNode6666.getLocalMember.status should be(MemberStatus.Up) }

      val values = clusterNode6666.groupValuesByHostingNodePort(List("v1", "v2", "v3", "v4", "v5", "v6"))
      values(6666).size should be > 1

      // Add a value to first cluster node
      val headValue = values(6666).head
      val headKey = Await.result(clusterNode6666.service.addRemoteValue(headValue), awaitDuration)
      val actual = Await.result(clusterNode6666.service.getRemoteValue(headKey), awaitDuration)
      actual should be(headValue)

      // Start second cluster node
      val clusterNode8888 = f.createClusterNode(8888, naturalRingReplicas = true)
      eventually { clusterNode8888.getLocalMember.status should be(MemberStatus.Up) }

      // Wait until the value is replicated to the shard slave
      eventually { clusterNode8888.service.getLocalValue(headKey.timestamp) should be(Some(headValue)) }

      // Change master for the first service member
      val token = clusterNode6666.getLocalMember.token
      f.consistencyPersistence.changeMasterServiceMember(token, clusterNode8888.localNode)
      eventually {
        val member = clusterNode6666.getMemberByToken(token)
        member.node should be(clusterNode8888.localNode)
        member.status should be(MemberStatus.Up)
      }
      val actual2 = Await.result(clusterNode6666.service.getRemoteValue(headKey), awaitDuration)
      actual2 should be(headValue)

      // Add remaining values to the migrated service member.
      val tailPairs = values(6666).tail.map(v => {
        val k = Await.result(clusterNode6666.service.addRemoteValue(v), awaitDuration)
        (k, v)
      })

      // Verify the remaining values are hosted only by node 8888, the new master. They are not replicated to node 6666 because
      // we are testing with a natural ring Resolver and both shards are now hosted by node 8888.
      tailPairs.foreach { case (k, v) => clusterNode8888.service.getLocalValue(k.timestamp) should be(Some(v)) }
      tailPairs.foreach { case (k, v) => clusterNode6666.service.getLocalValue(k.timestamp) should be(None) }
    }
  }

  ignore should "handle service members split (explicit replicas mapping)" in new ClusterFixture {
    withFixture { f =>
    // Start cluster nodes with assigned service members
      val clusterNode6666 = f.createClusterNode(6666, naturalRingReplicas = false)
      val clusterNode8888 = f.createClusterNode(8888, naturalRingReplicas = false)
      eventually {
        clusterNode6666.getLocalMember.status should be(MemberStatus.Up)
        clusterNode8888.getLocalMember.status should be(MemberStatus.Up)
      }

      // Add all values
      val values = clusterNode6666.groupValuesByHostingNodePort(List("v1", "v2", "v3", "v4", "v5", "v6", "v7", "v8"))
      val keyedValues6666 = values(6666).map(v => {
        val k = Await.result(clusterNode6666.service.addRemoteValue(v), awaitDuration)
        (k, v)
      })
      val keyedValues8888 = values(8888).map(v => {
        val k = Await.result(clusterNode8888.service.addRemoteValue(v), awaitDuration)
        (k, v)
      })

      // Start service member slaves i.e. the ones without assigned service members and wait until they are boot strapped
      val clusterNode5555 = f.createClusterNode(5555, naturalRingReplicas = false)
      val clusterNode7777 = f.createClusterNode(7777, naturalRingReplicas = false)
      eventually {
        keyedValues6666.foreach { case (k, v) => clusterNode5555.service.getLocalValue(k.timestamp) should be(Some(v)) }
        keyedValues8888.foreach { case (k, v) => clusterNode7777.service.getLocalValue(k.timestamp) should be(Some(v)) }
      }

      ///////////////////////////////////////////////////////////////////////////////////////////////////////////////
      // NOTE: Live migration is not really supported. The service member master MUST be stop before the 
      // split to ensure all values are replicated to the slave (i.e. no new values are during the split which would 
      // not be replicated because of the replication delay). Also ConsistencyMasterSlave does not properly 
      // terminate existing replication sessions if the master is not restarted.
      ///////////////////////////////////////////////////////////////////////////////////////////////////////////////

      // Stop existing master nodes
      clusterNode6666.stop()
      clusterNode8888.stop()
      eventually {
        clusterNode6666.getLocalMember.status should be(MemberStatus.Down)
        clusterNode8888.getLocalMember.status should be(MemberStatus.Down)
      }

      // Split shards!!!!
      f.serviceCache.addMember(f.member1073741823_5555)
      f.serviceCache.addMember(f.member3221225469_7777)

      // Wait until the new service members are Up
      eventually {
        val member5555 = clusterNode5555.getMemberByToken(f.member1073741823_5555.token)
        member5555.node should be(clusterNode5555.localNode)
        member5555.status should be(MemberStatus.Up)

        val member7777 = clusterNode7777.getMemberByToken(f.member3221225469_7777.token)
        member7777.node should be(clusterNode7777.localNode)
        member7777.status should be(MemberStatus.Up)
      }

      // Add new values to one of the new service member
      val values2 = clusterNode6666.groupValuesByHostingNodePort(List("z1", "z2", "z3", "z4", "z5", "z6", "z7"))
      values2(5555).size should be > 1
      val keyedValues5555 = values2(5555).map(v => {
        val k = Await.result(clusterNode5555.service.addRemoteValue(v), awaitDuration)
        val actual = Await.result(clusterNode5555.service.getRemoteValue(k), awaitDuration)
        actual should be(v)
        (k, v)
      })

      // Start old master and ensure the values are replicated
      val clusterNode6666_2 = f.createClusterNode(6666) // TODO: restart the existing node instance
      eventually {
        // TODO: There is a consistency error because the new node is empty but not the transaction logs!!!
        // TODO: Either ensure that dummy consistent storage data survive cluster node restart or recreate the data it!
        clusterNode6666_2.getLocalMember.status should be(MemberStatus.Up)
        keyedValues5555.foreach { case (k, v) => clusterNode6666_2.service.getLocalValue(k.timestamp) should be(Some(v)) }
      }

    }
  }

}
