package com.wajam.nrv.consistency

import scala.language.postfixOps
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
import java.util.concurrent.TimeoutException
import com.wajam.nrv.UnavailableException
import org.apache.commons.io.FileUtils
import com.wajam.nrv.consistency.replication.ReplicationMode

@RunWith(classOf[JUnitRunner])
class TestConsistencyMasterSlave extends FlatSpec with Matchers with Eventually with IntegrationPatience {

  trait ClusterFixture {

    val node5555 = Node.fromString("localhost:nrv=5555")
    val node6666 = Node.fromString("localhost:nrv=6666")
    val node7777 = Node.fromString("localhost:nrv=7777")
    val node8888 = Node.fromString("localhost:nrv=8888")

    val member1073741823_5555 = new ServiceMember(1073741823L, node5555)
    val member2147483646_6666 = new ServiceMember(2147483646L, node6666)
    val member3221225469_7777 = new ServiceMember(3221225469L, node7777)
    val member4294967292_8888 = new ServiceMember(4294967292L, node8888)

    case class FixtureParam(logDir: File) {

      private var clusterNodes: List[ConsistentCluster] = Nil

      // Uses only two service member (i.e. shard) by default on nodes 6666 and 8888
      val clusterDefinitionStorage = new ServiceMemberClusterStorage("dummy-consistent-store")
      clusterDefinitionStorage.addMember(member2147483646_6666)
      clusterDefinitionStorage.addMember(member4294967292_8888)

      val consistencyPersistence = new DummyConsistencyPersistence(clusterDefinitionStorage, explicitReplicasMapping)

      def createClusterNode(localNrvPort: Int, naturalRingReplicas: Boolean = true,
                            localStorage: TransactionStorage = new TransactionStorage, replicationRate: Int = 50): ConsistentCluster = {
        val nodeLogDir = new File(logDir, localNrvPort.toString)
        Files.createDirectories(nodeLogDir.toPath)
        val clusterNode = new ConsistentCluster(clusterDefinitionStorage, localStorage, consistencyPersistence, timestampGenerator,
          localNrvPort, nodeLogDir.getCanonicalPath, naturalRingReplicas, replicationRate)
        clusterNodes = clusterNode :: clusterNodes
        clusterNode.start()
        clusterNode
      }

      def stop(): Unit = {
        clusterNodes.foreach(_.stop())
      }
    }

    def explicitReplicasMapping: Map[Long, List[Node]] = Map(
      member2147483646_6666.token -> List(node6666, node8888),
      member4294967292_8888.token -> List(node8888, node6666)
    )

    val timestampGenerator = new DummyTimestampGenerator

    val awaitDuration: Duration = 3 seconds

    implicit val ec = ExecutionContext.global

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

  class ConsistentCluster(clusterDefinitionStorage: ServiceMemberClusterStorage,
                          localStorage: TransactionStorage,
                          consistencyPersistence: ConsistencyPersistence,
                          timestampGenerator: TimestampGenerator,
                          val localNrvPort: Int,
                          logDir: String,
                          naturalRingReplicas: Boolean,
                          replicationRate: Int) {

    val service = new DummyConsistentStoreService(clusterDefinitionStorage.serviceName, localStorage, replicasCount = 2)

    val clusterManager = new DummyDynamicClusterManager(Map(clusterDefinitionStorage.serviceName -> clusterDefinitionStorage))

    val localNode = new LocalNode("localhost", Map("nrv" -> localNrvPort))

    val cluster = new Cluster(localNode, clusterManager)
    cluster.registerService(service)

    val consistency = new ConsistencyMasterSlave(timestampGenerator, consistencyPersistence, logDir,
      txLogEnabled = true, replicationTps = replicationRate, replicationResolver = replicationResolver)
    service.applySupport(consistency = Some(consistency))
    consistency.bindService(service)

    def getMemberByToken(token: Long): ServiceMember = service.members.find(m => m.token == token).get

    def getMembersByPort(nrvPort: Int): Iterable[ServiceMember] = {
      service.members.filter(m => m.node.ports("nrv") == nrvPort)
    }

    def getMemberByPort(nrvPort: Int): ServiceMember = {
      val members = getMembersByPort(nrvPort)
      members.size should be(1)
      members.head
    }

    def getLocalMembers = service.members.filter(_.node == cluster.localNode)

    def getLocalMember = {
      val members = getLocalMembers
      members.size should be(1)
      members.head
    }

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
      eventually { clusterNode8888.service.localStorage.getValue(headKey.timestamp) should be(Some(headValue)) }

      // Add remaining values to first cluster node
      val tailPairs = values(6666).tail.map(v => {
        val k = Await.result(clusterNode6666.service.addRemoteValue(v), awaitDuration)
        (k, v)
      })

      // Wait until all values are replicated
      eventually {
        tailPairs.foreach { case (k, v) => clusterNode8888.service.localStorage.getValue(k.timestamp) should be(Some(v)) }
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
      eventually { clusterNode8888.service.localStorage.getValue(headKey.timestamp) should be(Some(headValue)) }

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
      eventually { clusterNode8888.service.localStorage.getValue(headKey.timestamp) should be(Some(headValue)) }

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
      eventually { clusterNode8888.service.localStorage.getValue(headKey.timestamp) should be(Some(headValue)) }

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

  it should "automatically recover from consistency error when store consistency is restored" in new ClusterFixture {
    withFixture { f =>
      // Start the first cluster node
      val clusterNode6666 = f.createClusterNode(6666)
      eventually { clusterNode6666.getLocalMember.status should be(MemberStatus.Up) }

      val values = clusterNode6666.groupValuesByHostingNodePort(List("v1", "v2", "v3", "v4", "v5", "v6"))
      values(6666).size should be > 1

      // Add a value to first cluster node
      val headValue = values(6666).head
      val headKey = Await.result(clusterNode6666.service.addRemoteValue(headValue), awaitDuration)
      val actual = Await.result(clusterNode6666.service.getRemoteValue(headKey), awaitDuration)
      actual should be(headValue)

      // Remove the value directly from the local consistent store without going thru NRV service messaging.
      // This break the consistency between the store and the transaction logs.
      // This prevents the service status to goes Up when the cluster node is restarted
      val removedValue = clusterNode6666.service.localStorage.remove(headKey.timestamp).get
      val token = clusterNode6666.getLocalMember.token

      // Force consistency check
      f.clusterDefinitionStorage.setMemberDown(token)
      eventually { clusterNode6666.getLocalMemberConsistencyState(token) should be(Some(MemberConsistencyState.Error)) }

      // Restore the deleted value. The service consistency should recover and the service status goes Up
      clusterNode6666.service.localStorage.add(removedValue)
      eventually { clusterNode6666.getLocalMember.status should be(MemberStatus.Up) }
    }
  }

  it should "automatically recover from incomplete tx consistency error" in new ClusterFixture {
    withFixture { f =>
      val clusterNode6666 = f.createClusterNode(6666)
      eventually { clusterNode6666.getLocalMember.status should be(MemberStatus.Up) }

      val values = clusterNode6666.groupValuesByHostingNodePort(List("v1", "v2", "v3", "v4", "v5", "v6"))
      values(6666).size should be > 1

      // Add the service member tail values (i.e. all values but the first one)
      values(6666).tail.map(v => Await.result(clusterNode6666.service.addRemoteValue(v), awaitDuration))

      // Increase the consistent service timeout to a value larger than the consistency timeout. The service will
      // become inconsistent if no reply is received before the consistency timeout expires.
      // The consistency timeout is based on the service timeout but it is computed once the service goes Up.
      // Increasing the service timeout AFTER the service is Up does not update the consistency timeout.
      clusterNode6666.service.applySupport(responseTimeout = Some(30000L))
      evaluating {
        Await.result(clusterNode6666.service.addRemoteValue(values(6666).head, mustReply = false), awaitDuration)
      } should produce[TimeoutException]

      val token = clusterNode6666.getLocalMember.token
      eventually {
        clusterNode6666.getLocalMemberConsistencyState(token) should not be Some(MemberConsistencyState.Ok)
      }

      eventually {
        clusterNode6666.getLocalMember.status should be(MemberStatus.Up)
        clusterNode6666.getLocalMemberConsistencyState(token) should be(Some(MemberConsistencyState.Ok))
      }
    }
  }

  it should "handle service external member mastership migration" in new ClusterFixture {
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
      eventually { clusterNode8888.service.localStorage.getValue(headKey.timestamp) should be(Some(headValue)) }

      // Change master for the first service member
      val token = member2147483646_6666.token
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
      tailPairs.foreach { case (k, v) => clusterNode8888.service.localStorage.getValue(k.timestamp) should be(Some(v)) }
      tailPairs.foreach { case (k, v) => clusterNode6666.service.localStorage.getValue(k.timestamp) should be(None) }
    }
  }

  it should "handle offline service members split (explicit replicas mapping)" in new ClusterFixture {

    override def explicitReplicasMapping: Map[Long, List[Node]] = Map(
      member1073741823_5555.token -> List(node5555, node6666),
      member2147483646_6666.token -> List(node5555, node6666),
      member3221225469_7777.token -> List(node7777, node8888),
      member4294967292_8888.token -> List(node7777, node8888)
    )

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
        keyedValues6666.foreach { case (k, v) =>
          clusterNode5555.service.localStorage.getValue(k.timestamp) should be(Some(v)) }
        keyedValues8888.foreach { case (k, v) =>
          clusterNode7777.service.localStorage.getValue(k.timestamp) should be(Some(v)) }
      }

      ///////////////////////////////////////////////////////////////////////////////////////////////////////////////
      // NOTE: Live split to a new service member master is not really supported:
      //
      // 1. The service member master MUST be stop before the split to ensure all values are replicated to the slave
      // (i.e. no new values are during the split which would not be replicated because of the replication delay).
      //
      // 2. ConsistencyMasterSlave does not properly terminate existing replication sessions if the master is not
      // restarted.
      ///////////////////////////////////////////////////////////////////////////////////////////////////////////////

      // Stop existing master nodes
      clusterNode6666.stop()
      clusterNode8888.stop()
      eventually {
        clusterNode6666.getLocalMember.status should be(MemberStatus.Down)
        clusterNode8888.getLocalMember.status should be(MemberStatus.Down)
      }

      // Split shards!!!!
      f.clusterDefinitionStorage.addMember(member1073741823_5555)
      f.clusterDefinitionStorage.addMember(member3221225469_7777)

      // Wait until the new service members are Up
      eventually {
        val member5555 = clusterNode5555.getMemberByToken(member1073741823_5555.token)
        member5555.node should be(clusterNode5555.localNode)
        member5555.status should be(MemberStatus.Up)

        val member7777 = clusterNode7777.getMemberByToken(member3221225469_7777.token)
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
      val clusterNode6666_2 = f.createClusterNode(6666, localStorage = clusterNode6666.service.localStorage) // TODO: restart the existing node instance
      eventually {
        clusterNode6666_2.getLocalMember.status should be(MemberStatus.Up)
        keyedValues5555.foreach { case (k, v) =>
          clusterNode6666_2.service.localStorage.getValue(k.timestamp) should be(None) }
      }
    }
  }

  it should "migrate mastership of offline master when slave is in-sync" in new ClusterFixture {
    withFixture { f =>

    // Startup both cluster nodes
      val clusterNode6666 = f.createClusterNode(6666, naturalRingReplicas = false)
      val clusterNode8888 = f.createClusterNode(8888, naturalRingReplicas = false)
      eventually {
        clusterNode6666.getLocalMember.status should be(MemberStatus.Up)
        clusterNode8888.getLocalMember.status should be(MemberStatus.Up)
      }

      // Add all values and ensure they are replicated
      val values = clusterNode6666.groupValuesByHostingNodePort(List("v1", "v2", "v3", "v4", "v5", "v6", "v7", "v8"))
      val keyedValues6666 = values(6666).map(v => {
        val k = Await.result(clusterNode6666.service.addRemoteValue(v), awaitDuration)
        (k, v)
      })
      val keyedValues8888 = values(8888).map(v => {
        val k = Await.result(clusterNode8888.service.addRemoteValue(v), awaitDuration)
        (k, v)
      })
      eventually {
        keyedValues6666.foreach { case (k, v) =>
          clusterNode8888.service.localStorage.getValue(k.timestamp) should be(Some(v))
        }
        keyedValues8888.foreach { case (k, v) =>
          clusterNode6666.service.localStorage.getValue(k.timestamp) should be(Some(v))
        }
      }

      // Verify the master consistent timestamp before the migration
      val ranges = ResolvedServiceMember(clusterNode8888.service, clusterNode8888.getLocalMember).ranges
      ranges.size should be(1)
      eventually {
        clusterNode8888.service.getCurrentConsistentTimestamp(ranges.head) should be(clusterNode8888.service.getLastTimestamp(ranges))
      }

      // Stop one node
      val token = member4294967292_8888.token
      clusterNode8888.stop()
      eventually {
        clusterNode6666.getMemberByToken(token).status should be(MemberStatus.Down)
      }

      // Verify writing a value hosted by the stopped node doesn't work
      val values2 = clusterNode6666.groupValuesByHostingNodePort(List("z1", "z2", "z3", "z4", "z5", "z6", "z7"))
      values2(8888).size should be > 1
      evaluating {
        Await.result(clusterNode6666.service.addRemoteValue(values2(8888).head), awaitDuration)
      } should produce[UnavailableException]

      // Change mastership from the stopped node to the started one
      Await.result(clusterNode6666.consistency.changeMasterServiceMember(token, node6666), awaitDuration)
      eventually { clusterNode6666.getMemberByToken(token).status should be(MemberStatus.Up) }

      // Verify the new master consistent timestamp for the migrated shard
      eventually {
        clusterNode6666.service.getCurrentConsistentTimestamp(ranges.head) should be(clusterNode8888.service.getLastTimestamp(ranges))
      }

      // Should now be able to write on new master node
      values2(8888).map(v => {
        val k = Await.result(clusterNode6666.service.addRemoteValue(v), awaitDuration)
        val actual = Await.result(clusterNode6666.service.getRemoteValue(k), awaitDuration)
        actual should be(v)
      })
    }
  }

  it should "migrate mastership of offline master when slave is out-of-sync only when force flag is in effect" in new ClusterFixture {
    withFixture { f =>
    // Startup both cluster nodes
      val clusterNode6666 = f.createClusterNode(6666, naturalRingReplicas = false)
      val clusterNode8888 = f.createClusterNode(8888, naturalRingReplicas = false)
      eventually {
        clusterNode6666.getLocalMember.status should be(MemberStatus.Up)
        clusterNode8888.getLocalMember.status should be(MemberStatus.Up)
      }

      // Add all values and ensure they are replicated
      val values = clusterNode6666.groupValuesByHostingNodePort(List("v1", "v2", "v3", "v4", "v5", "v6", "v7", "v8"))
      val keyedValues6666 = values(6666).map(v => {
        val k = Await.result(clusterNode6666.service.addRemoteValue(v), awaitDuration)
        (k, v)
      })
      val keyedValues8888 = values(8888).map(v => {
        val k = Await.result(clusterNode8888.service.addRemoteValue(v), awaitDuration)
        (k, v)
      })
      eventually {
        keyedValues6666.foreach { case (k, v) =>
          clusterNode8888.service.localStorage.getValue(k.timestamp) should be(Some(v))
        }
        keyedValues8888.foreach { case (k, v) =>
          clusterNode6666.service.localStorage.getValue(k.timestamp) should be(Some(v))
        }
      }

      // Verify the master consistent timestamp before the migration
      val ranges = ResolvedServiceMember(clusterNode8888.service, clusterNode8888.getLocalMember).ranges
      ranges.size should be(1)
      eventually {
        clusterNode8888.service.getCurrentConsistentTimestamp(ranges.head) should be(clusterNode8888.service.getLastTimestamp(ranges))
      }

      // Stop one node
      val token = clusterNode8888.getLocalMember.token
      clusterNode8888.stop()
      eventually {
        clusterNode6666.getMemberByToken(token).status should be(MemberStatus.Down)
      }

      // Verify writing a value hosted by the stopped node doesn't work
      val values2 = clusterNode6666.groupValuesByHostingNodePort(List("z1", "z2", "z3", "z4", "z5", "z6", "z7"))
      values2(8888).size should be > 1
      evaluating {
        Await.result(clusterNode6666.service.addRemoteValue(values2(8888).head), awaitDuration)
      } should produce[UnavailableException]

      // Try to change mastership from the stopped node to the started one with out-of-sync lag
      f.consistencyPersistence.updateReplicationLagSeconds(token, node6666, lag = 5)
      evaluating {
        Await.result(clusterNode6666.consistency.changeMasterServiceMember(token, node6666), awaitDuration)
      } should produce[IllegalStateException]

      // Try again but force migration this time
      Await.result(clusterNode6666.consistency.changeMasterServiceMember(token, node6666, forceOfflineMigration = true), awaitDuration)
      eventually { clusterNode6666.getMemberByToken(token).status should be(MemberStatus.Up) }

      // Verify the new master consistent timestamp for the migrated shard
      eventually {
        clusterNode6666.service.getCurrentConsistentTimestamp(ranges.head) should be(clusterNode8888.service.getLastTimestamp(ranges))
      }

      // Should now be able to write on new master node
      values2(8888).map(v => {
        val k = Await.result(clusterNode6666.service.addRemoteValue(v), awaitDuration)
        val actual = Await.result(clusterNode6666.service.getRemoteValue(k), awaitDuration)
        actual should be(v)
      })

    }
  }

  it should "migrate mastership of local online master when slave is in-sync" in new ClusterFixture {
    withFixture { f =>

      // Use a reduced replication rate (i.e. 1 tps) to ensure replication still ongoing when trying the migration
      val clusterNode6666 = f.createClusterNode(6666, naturalRingReplicas = false, replicationRate = 1)
      val clusterNode8888 = f.createClusterNode(8888, naturalRingReplicas = false)
      eventually {
        clusterNode6666.getLocalMember.status should be(MemberStatus.Up)
        clusterNode8888.getLocalMember.status should be(MemberStatus.Up)
      }

      val values = clusterNode6666.groupValuesByHostingNodePort(List("v1", "v2", "v3", "v4", "v5", "v6", "v7", "v8"))

      // Add a value to first cluster node
      val headValue = values(8888).head
      val headKey = Await.result(clusterNode8888.service.addRemoteValue(headValue), awaitDuration)
      val actual = Await.result(clusterNode8888.service.getRemoteValue(headKey), awaitDuration)
      actual should be(headValue)

      // Wait that the first value is replicated and live replication mode is in effect
      eventually {
        clusterNode6666.service.localStorage.getValue(headKey.timestamp) should be(Some(headValue))
        val session = clusterNode8888.consistency.replicationSessions.find(_.slave == node6666)
        session.get.mode should be(ReplicationMode.Live)
      }

      // Add remaining values to the cluster node
      val tailPairs = values(8888).tail.map(v => {
        val k = Await.result(clusterNode8888.service.addRemoteValue(v), awaitDuration)
        (k, v)
      })

      // Try live mastership migration from the master node while the remaining values are not all replicated yet
      val token = member4294967292_8888.token
      val migrationFuture = clusterNode8888.consistency.changeMasterServiceMember(token, node6666)

      // Try migration a second time while the initial migration request is not completed yet (should fail)
      evaluating {
        Await.result(clusterNode8888.consistency.changeMasterServiceMember(token, node6666), awaitDuration)
      } should produce[IllegalStateException]
      migrationFuture.isCompleted should be(false)

      // Wait for the migration to complete
      Await.result(migrationFuture, 5 seconds)
      eventually {
        val member = clusterNode8888.getMemberByToken(token)
        member.node should be(node6666)
        member.status should be(MemberStatus.Up)

        // verify that all values have been replicated during the mastership migration
        tailPairs.foreach { case (k, v) => clusterNode6666.service.localStorage.getValue(k.timestamp) should be(Some(v)) }
      }

      // Migrate mastership back to the original master (after replication mode is live again)
      eventually {
        val session = clusterNode6666.consistency.replicationSessions.find(_.slave == node8888)
        session.get.mode should be(ReplicationMode.Live)
      }
      Await.result(clusterNode6666.consistency.changeMasterServiceMember(token, node8888), awaitDuration)
      eventually {
        val member = clusterNode6666.getMemberByToken(token)
        member.node should be(node8888)
        member.status should be(MemberStatus.Up)
      }

      // Migrate yet again! This is to ensure the original migration state is clear and
      // does not prevent migrating again from the original master
      eventually {
        val session = clusterNode8888.consistency.replicationSessions.find(_.slave == node6666)
        session.get.mode should be(ReplicationMode.Live)
      }
      Await.result(clusterNode8888.consistency.changeMasterServiceMember(token, node6666), awaitDuration)
      eventually {
        val member = clusterNode8888.getMemberByToken(token)
        member.node should be(node6666)
        member.status should be(MemberStatus.Up)
      }
    }
  }

  it should "NOT migrate mastership of remote online master" in new ClusterFixture {
    withFixture { f =>
      val clusterNode6666 = f.createClusterNode(6666, naturalRingReplicas = false)
      val clusterNode8888 = f.createClusterNode(8888, naturalRingReplicas = false)
      eventually {
        clusterNode6666.getLocalMember.status should be(MemberStatus.Up)
        clusterNode8888.getLocalMember.status should be(MemberStatus.Up)
      }

      val values = clusterNode6666.groupValuesByHostingNodePort(List("v1", "v2", "v3", "v4", "v5", "v6", "v7", "v8"))

      // Add a value to first cluster node
      val headValue = values(8888).head
      val headKey = Await.result(clusterNode8888.service.addRemoteValue(headValue), awaitDuration)
      val actual = Await.result(clusterNode8888.service.getRemoteValue(headKey), awaitDuration)
      actual should be(headValue)

      // Wait that the first value is replicated and live replication mode is in effect
      eventually {
        clusterNode6666.service.localStorage.getValue(headKey.timestamp) should be(Some(headValue))
        val session = clusterNode8888.consistency.replicationSessions.find(_.slave == node6666)
        session.get.mode should be(ReplicationMode.Live)
      }

      // Try live mastership migration from slave node instead of from the master node.
      evaluating {
        Await.result(clusterNode6666.consistency.changeMasterServiceMember(member4294967292_8888.token, node6666), awaitDuration)
      } should produce[IllegalArgumentException]
    }
  }

  it should "NOT migrate mastership of local online master when slave is out-of-sync" in new ClusterFixture {
    withFixture { f =>
      val clusterNode6666 = f.createClusterNode(6666, naturalRingReplicas = false)
      val clusterNode8888 = f.createClusterNode(8888, naturalRingReplicas = false)
      eventually {
        clusterNode6666.getLocalMember.status should be(MemberStatus.Up)
        clusterNode8888.getLocalMember.status should be(MemberStatus.Up)
      }

      val values = clusterNode6666.groupValuesByHostingNodePort(List("v1", "v2", "v3", "v4", "v5", "v6", "v7", "v8"))

      // Add a value to first cluster node
      val headValue = values(8888).head
      val headKey = Await.result(clusterNode8888.service.addRemoteValue(headValue), awaitDuration)
      val actual = Await.result(clusterNode8888.service.getRemoteValue(headKey), awaitDuration)
      actual should be(headValue)

      // Wait that the first value is replicated and live replication mode is in effect
      eventually {
        clusterNode6666.service.localStorage.getValue(headKey.timestamp) should be(Some(headValue))
        val session = clusterNode8888.consistency.replicationSessions.find(_.slave == node6666)
        session.get.mode should be(ReplicationMode.Live)
      }

      // Try live mastership migration when slave is out-of-sync
      val token = member4294967292_8888.token
      f.consistencyPersistence.updateReplicationLagSeconds(token, node6666, lag = 5)
      evaluating {
        Await.result(clusterNode8888.consistency.changeMasterServiceMember(token, node6666), awaitDuration)
      } should produce[IllegalStateException]
    }
  }

  it should "fail mastership migration if service member token is invalid" in new ClusterFixture {
    withFixture { f =>
      val clusterNode6666 = f.createClusterNode(6666, naturalRingReplicas = false)
      val clusterNode8888 = f.createClusterNode(8888, naturalRingReplicas = false)
      eventually {
        clusterNode6666.getLocalMember.status should be(MemberStatus.Up)
        clusterNode8888.getLocalMember.status should be(MemberStatus.Up)
      }

      val values = clusterNode6666.groupValuesByHostingNodePort(List("v1", "v2", "v3", "v4", "v5", "v6", "v7", "v8"))

      // Add a value to first cluster node
      val headValue = values(8888).head
      val headKey = Await.result(clusterNode8888.service.addRemoteValue(headValue), awaitDuration)
      val actual = Await.result(clusterNode8888.service.getRemoteValue(headKey), awaitDuration)
      actual should be(headValue)

      // Wait that the first value is replicated and live replication mode is in effect
      eventually {
        clusterNode6666.service.localStorage.getValue(headKey.timestamp) should be(Some(headValue))
        val session = clusterNode8888.consistency.replicationSessions.find(_.slave == node6666)
        session.get.mode should be(ReplicationMode.Live)
      }

      // Try live mastership migration when slave is out-of-sync
      val invalidToken = member4294967292_8888.token + 1
      evaluating {
        Await.result(clusterNode8888.consistency.changeMasterServiceMember(invalidToken, node6666), awaitDuration)
      } should produce[IllegalArgumentException]
    }
  }

  it should "fail mastership migration if target node is not a replica" in new ClusterFixture {
    withFixture { f =>
      val clusterNode6666 = f.createClusterNode(6666, naturalRingReplicas = false)
      val clusterNode8888 = f.createClusterNode(8888, naturalRingReplicas = false)
      eventually {
        clusterNode6666.getLocalMember.status should be(MemberStatus.Up)
        clusterNode8888.getLocalMember.status should be(MemberStatus.Up)
      }

      val values = clusterNode6666.groupValuesByHostingNodePort(List("v1", "v2", "v3", "v4", "v5", "v6", "v7", "v8"))

      // Add a value to first cluster node
      val headValue = values(8888).head
      val headKey = Await.result(clusterNode8888.service.addRemoteValue(headValue), awaitDuration)
      val actual = Await.result(clusterNode8888.service.getRemoteValue(headKey), awaitDuration)
      actual should be(headValue)

      // Wait that the first value is replicated and live replication mode is in effect
      eventually {
        clusterNode6666.service.localStorage.getValue(headKey.timestamp) should be(Some(headValue))
        val session = clusterNode8888.consistency.replicationSessions.find(_.slave == node6666)
        session.get.mode should be(ReplicationMode.Live)
      }

      // Try live mastership migration when slave is out-of-sync
      val token = member4294967292_8888.token
      val nonReplicaNode = node5555
      evaluating {
        Await.result(clusterNode8888.consistency.changeMasterServiceMember(token, nonReplicaNode), awaitDuration)
      } should produce[IllegalArgumentException]
    }
  }

  // TODO: Try concurrent migration to multiple valid replicas.
  // TODO: Local node is not the master replica anymore.
  // TODO: Target node is not a valid replica anymore.

}
