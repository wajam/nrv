package com.wajam.nrv.consistency

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{Matchers, FlatSpec}
import com.wajam.nrv.service.{Resolver, MemberStatus, ServiceMember}
import com.wajam.nrv.cluster.{LocalNode, Cluster}
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

      val m_1073741823_6666 = "1073741823:localhost:nrv=6666"
      val m_2147483646_6666 = "2147483646:localhost:nrv=6666"
      val m_3221225469_8888 = "3221225469:localhost:nrv=8888"
      val m_4294967292_8888 = "2147483646:localhost:nrv=8888"

      val serviceCache = new ServiceMemberClusterStorage("dummy-consistent-store")
      //serviceCache.addMember(ServiceMember.fromString(m_1073741823_6666))
      serviceCache.addMember(ServiceMember.fromString(m_2147483646_6666))
      //serviceCache.addMember(ServiceMember.fromString(m_3221225469_8888))
      serviceCache.addMember(ServiceMember.fromString(m_4294967292_8888))

      val consistencyPersistence = new DummyConsistencyPersistence(serviceCache)

      def createClusterNode(localNrvPort: Int): ConsistentCluster = {
        val nodeLogDir = new File(logDir, localNrvPort.toString)
        Files.createDirectories(nodeLogDir.toPath)
        val clusterNode = new ConsistentCluster(serviceCache, consistencyPersistence, timestampGenerator,
          localNrvPort, nodeLogDir.getCanonicalPath)
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

  class ConsistentCluster(serviceCache: ServiceMemberClusterStorage, consistencyPersistence:ConsistencyPersistence,
                          timestampGenerator: TimestampGenerator, localNrvPort: Int, logDir: String) {
    val service = new DummyConsistentStoreService(serviceCache.serviceName, replicasCount = 2)

    val clusterManager = new DummyDynamicClusterManager(Map(serviceCache.serviceName -> serviceCache))

    val localNode = new LocalNode("localhost", Map("nrv" -> localNrvPort))

    val cluster = new Cluster(localNode, clusterManager)
    cluster.registerService(service)

    val consistency = new ConsistencyMasterSlave(timestampGenerator, consistencyPersistence, logDir, txLogEnabled = true)
    service.applySupport(consistency = Some(consistency))
    consistency.bindService(service)

    def getMemberByToken(token: Long): ServiceMember = service.members.find(m => m.token == token).get

    def getMemberByPort(nrvPort: Int): ServiceMember = service.members.find(m => m.node.ports("nrv") == nrvPort).get

    def getLocalMember = getMemberByPort(localNrvPort)

    def getLocalMemberConsistencyState(token: Long): Option[MemberConsistencyState] = {
      consistency.localMembersStates.collectFirst{case (m, s) if m.token == token => s}
    }

    def getHostingNodePortForValue(value: String): Int = service.resolveMembers(Resolver.hashData(value), 1).head.node.ports("nrv")

    def groupValuesByHostingNodePort(values: List[String]): Map[Int, List[String]] = {
      val grouped: Map[Int, List[(Int, String)]] = values.map(v => (getHostingNodePortForValue(v), v)).groupBy(_._1)
      grouped.map{ case (port, tupledValues) => (port, tupledValues.map(_._2))}
    }

    def start(): Unit = {
      cluster.start()
    }

    def stop(): Unit = {
      cluster.stop(5000)
    }
  }

//  "ConsistencyMasterSlave" should "replicate master values to slave" in new ClusterFixture {
  ignore should "replicate master values to slave" in new ClusterFixture {
    withFixture { f =>
      // Start first custer node
      val clusterNode6666 = f.createClusterNode(6666)
      eventually {clusterNode6666.getLocalMember.status should be(MemberStatus.Up)}

      val values = clusterNode6666.groupValuesByHostingNodePort(List("v1", "v2", "v3", "v4", "v5", "v6"))
      values(6666).size should be > 1

      // Add a value to first cluster node
      val headValue = values(6666).head
      val headKey = Await.result(clusterNode6666.service.addRemoteValue(headValue), awaitDuration)
      val actual = Await.result(clusterNode6666.service.getRemoteValue(headKey), awaitDuration)
      actual should be(headValue)

      // Start second cluster node
      val clusterNode8888 = f.createClusterNode(8888)
      eventually {clusterNode8888.getLocalMember.status should be(MemberStatus.Up)}

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

  ignore should "allow read from slave when master is down" in new ClusterFixture {
    withFixture { f =>
      // Start first custer node
      val clusterNode6666 = f.createClusterNode(6666)
      eventually {clusterNode6666.getLocalMember.status should be(MemberStatus.Up)}

      val values = clusterNode6666.groupValuesByHostingNodePort(List("v1", "v2", "v3", "v4", "v5", "v6"))
      values(6666).size should be > 1

      // Add a value to first cluster node
      val headValue = values(6666).head
      val headKey = Await.result(clusterNode6666.service.addRemoteValue(headValue), awaitDuration)
      val actual = Await.result(clusterNode6666.service.getRemoteValue(headKey), awaitDuration)
      actual should be(headValue)

      // Start second cluster node
      val clusterNode8888 = f.createClusterNode(8888)
      eventually {clusterNode8888.getLocalMember.status should be(MemberStatus.Up)}

      // Wait until the value is replicated to the shard slave
      eventually { clusterNode8888.service.getLocalValue(headKey.timestamp) should be(Some(headValue)) }

      // Stop first cluster node
      clusterNode6666.stop()
      eventually {clusterNode8888.getMemberByPort(6666).status should be(MemberStatus.Down)}

      // Get the value, it should be served by the slave
      val actual2 = Await.result(clusterNode8888.service.getRemoteValue(headKey), awaitDuration)
      actual2 should be(headValue)
    }
  }

  ignore should "NOT allow read from slave when master is down and lag too large" in new ClusterFixture {
    withFixture { f =>
    // Start first custer node
      val clusterNode6666 = f.createClusterNode(6666)
      eventually {clusterNode6666.getLocalMember.status should be(MemberStatus.Up)}

      val values = clusterNode6666.groupValuesByHostingNodePort(List("v1", "v2", "v3", "v4", "v5", "v6"))
      values(6666).size should be > 1

      // Add a value to first cluster node
      val headValue = values(6666).head
      val headKey = Await.result(clusterNode6666.service.addRemoteValue(headValue), awaitDuration)
      val actual = Await.result(clusterNode6666.service.getRemoteValue(headKey), awaitDuration)
      actual should be(headValue)

      // Start second cluster node
      val clusterNode8888 = f.createClusterNode(8888)
      eventually {clusterNode8888.getLocalMember.status should be(MemberStatus.Up)}

      // Wait until the value is replicated to the shard slave
      eventually { clusterNode8888.service.getLocalValue(headKey.timestamp) should be(Some(headValue)) }

      // Stop first cluster node
      clusterNode6666.stop()
      eventually {clusterNode8888.getMemberByPort(6666).status should be(MemberStatus.Down)}

      // Artificially increase the replication lag before trying to get the value
      val masterToken = clusterNode6666.getLocalMember.token
      f.consistencyPersistence.updateReplicationLagSeconds(masterToken, clusterNode8888.localNode, 2)
      evaluating {
        Await.result(clusterNode8888.service.getRemoteValue(headKey), awaitDuration)
      } should produce[UnavailableException]
    }
  }

  ignore should "NOT allow write on slave when master is down" in new ClusterFixture {
    withFixture { f =>
    // Start first custer node
      val clusterNode6666 = f.createClusterNode(6666)
      eventually {clusterNode6666.getLocalMember.status should be(MemberStatus.Up)}

      val values = clusterNode6666.groupValuesByHostingNodePort(List("v1", "v2", "v3", "v4", "v5", "v6"))
      values(6666).size should be > 1

      // Add a value to first cluster node
      val headValue = values(6666).head
      val headKey = Await.result(clusterNode6666.service.addRemoteValue(headValue), awaitDuration)
      val actual = Await.result(clusterNode6666.service.getRemoteValue(headKey), awaitDuration)
      actual should be(headValue)

      // Start second cluster node
      val clusterNode8888 = f.createClusterNode(8888)
      eventually {clusterNode8888.getLocalMember.status should be(MemberStatus.Up)}

      // Wait until the value is replicated to the shard slave
      eventually { clusterNode8888.service.getLocalValue(headKey.timestamp) should be(Some(headValue)) }

      // Stop first cluster node
      clusterNode6666.stop()
      eventually {clusterNode8888.getMemberByPort(6666).status should be(MemberStatus.Down)}

      // Try to write a value to the shard with master down
      evaluating {
        val value2 = values(6666).tail.head
        Await.result(clusterNode8888.service.addRemoteValue(value2), awaitDuration)
      } should produce[UnavailableException]
    }
  }

  ignore should "recover from consistency error when store consistency is restored" in new ClusterFixture {
    withFixture { f =>
      // Start the first cluster node
      val clusterNode6666 = f.createClusterNode(6666)
      eventually {clusterNode6666.getLocalMember.status should be(MemberStatus.Up)}

      val values = clusterNode6666.groupValuesByHostingNodePort(List("v1", "v2", "v3", "v4", "v5", "v6"))
      values(6666).size should be > 1

      // Add a value to first cluster node and stop it
      val headValue = values(6666).head
      val headKey = Await.result(clusterNode6666.service.addRemoteValue(headValue), awaitDuration)
      val actual = Await.result(clusterNode6666.service.getRemoteValue(headKey), awaitDuration)
      actual should be(headValue)

      clusterNode6666.stop()
      eventually {clusterNode6666.getMemberByPort(6666).status should be(MemberStatus.Down)}

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
      val clusterNode6666 = f.createClusterNode(6666)
      eventually {clusterNode6666.getLocalMember.status should be(MemberStatus.Up)}

      val values = clusterNode6666.groupValuesByHostingNodePort(List("v1", "v2", "v3", "v4", "v5", "v6"))
      values(6666).size should be > 1

      // Add a value to first cluster node
      val headValue = values(6666).head
      val headKey = Await.result(clusterNode6666.service.addRemoteValue(headValue), awaitDuration)
      val actual = Await.result(clusterNode6666.service.getRemoteValue(headKey), awaitDuration)
      actual should be(headValue)

      // Start second cluster node
      val clusterNode8888 = f.createClusterNode(8888)
      eventually {clusterNode8888.getLocalMember.status should be(MemberStatus.Up)}

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

}
