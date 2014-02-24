package com.wajam.nrv.zookeeper.consistency

import scala.concurrent.ExecutionContext.Implicits.global
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FlatSpec}
import org.scalatest.Matchers._
import org.scalatest.concurrent.Eventually
import akka.actor.ActorSystem
import com.wajam.nrv.service.{ServiceMember, Service}
import com.wajam.nrv.cluster.{Cluster, LocalNode, Node}
import com.wajam.nrv.zookeeper.{ZookeeperTestHelpers, ZookeeperClient}
import com.wajam.nrv.zookeeper.cluster.ZookeeperClusterManager
import com.wajam.commons.ControlableCurrentTime
import org.scalatest.concurrent.Eventually

@RunWith(classOf[JUnitRunner])
class TestZookeeperConsistencyPersistence extends FlatSpec with BeforeAndAfter with Eventually {

  implicit val as = ActorSystem("TestZookeeperConsistencyPersistence")
  
  val TEST_PATH = "/tests/consistencypersistence"

  var zkClient: ZookeeperClient = _
  var cluster: Cluster = _

  val lagUpdateThreshold = 60
  val lagUpdateSpacing = 30

  before {
    import com.wajam.nrv.zookeeper.ZookeeperClient.string2bytes

    val zkRootClient = new ZookeeperClient("127.0.0.1")
    zkRootClient.deleteRecursive(TEST_PATH)
    zkRootClient.ensureAllExists(TEST_PATH, "")
    zkRootClient.close()

    zkClient = new ZookeeperClient("127.0.0.1" + TEST_PATH)
  }

  after {
    zkClient.close()
    cluster.stop()
  }

  trait Builder extends ZookeeperTestHelpers {
    val zk = zkClient

    val localNode = new LocalNode("127.0.0.1", Map("nrv" -> (10000)))

    val clusterManager = new ZookeeperClusterManager(zk)
    cluster = new Cluster(localNode, clusterManager)

    val originalMapping = Map(
      0 -> List(
        new Node("localhost", Map("nrv" -> 12345)),
        new Node("localhost", Map("nrv" -> 12346))
      ),
      10 -> List(
        new Node("localhost", Map("nrv" -> 12347)),
        new Node("localhost", Map("nrv" -> 12348))
      )
    )

    val members = originalMapping.flatMap { case (token, nodes) =>
      nodes.map { node =>
        new ServiceMember(token, node)
      }
    }

    val service = new Service("test.service")

    cluster.registerService(service)

    zkCreateService(service)

    originalMapping.foreach { case (token, nodes) =>
      zkCreateReplicasList(service, token, nodes)

      nodes.foreach { node =>
        zkCreateReplicationLag(service, token, node, 0)
      }
    }

    members.foreach { member =>
      service.addMember(member, triggerEvent = false)
      zkCreateServiceMember(service, member)
    }

    cluster.start()

    val consistency = new ZookeeperConsistencyPersistence(zk, service, lagUpdateThreshold, lagUpdateSpacing) with ControlableCurrentTime

    def checkCachedAndPersistedLagValues(service: Service, token: Long, slave: Node, value: Int) {
      consistency.replicationLagSeconds(token, slave) should be(Some(value))
      zkGetReplicationLag(service, token, slave) should be(value)
    }
  }

  it should "load the replica mapping from Zk on start" in new Builder {
    consistency.start()

    // Check for content equality
    consistency.explicitReplicasMapping should be(originalMapping)
    // Check for reference equality (ensures caching)
    consistency.explicitReplicasMapping should be theSameInstanceAs(consistency.explicitReplicasMapping)
  }

  it should "update the replica mapping when one of the replica lists changes" in new Builder {
    consistency.start()

    // Add a replica for token 0
    val newReplica = new Node("localhost", Map("nrv" -> 12349))
    val newMapping = originalMapping + (0 -> (newReplica :: originalMapping(0)))

    // Update mapping in Zk
    zkUpdateReplicasList(service, 0, newMapping(0))

    eventually {
      consistency.explicitReplicasMapping should be(newMapping)
    }
  }

  it should "update the replica mapping when a service member is added" in new Builder {
    consistency.start()

    // Add a service member for token 20
    val newMember = new ServiceMember(20, new Node("localhost", Map("nrv" -> 12349)))

    zkCreateReplicasList(service, 20, List(newMember.node))
    zkCreateServiceMember(service, newMember)

    // Trigger a NewMemberAddedEvent
    service.addMember(newMember, triggerEvent = true)

    val newMapping = originalMapping + (20 -> List(newMember.node))

    eventually {
      consistency.explicitReplicasMapping should be(newMapping)
    }
  }

  it should "watch for the replicas and the lag when a service member is added" in new Builder {
    consistency.start()

    // Add a service member for token 20, without setting replicas yet
    val newMember = new ServiceMember(20, new Node("localhost", Map("nrv" -> 12349)))

    zkCreateServiceMember(service, newMember)

    // Trigger a NewMemberAddedEvent
    service.addMember(newMember, triggerEvent = true)

    eventually {
      // At this point, the new token exists in the map but there are no replicas
      consistency.explicitReplicasMapping should be(originalMapping + (20 -> Nil))
    }

    // Set the replicas list
    zkCreateReplicasList(service, 20, List(newMember.node))

    eventually {
      // The replicas should now appear in the mapping
      consistency.explicitReplicasMapping should be(originalMapping + (20 -> List(newMember.node)))
    }

    val newLag = 12345

    // Create the lag for the new replica
    zkCreateReplicationLag(service, newMember.token, newMember.node, newLag)

    eventually {
      // The lag should now be cached
      checkCachedAndPersistedLagValues(service, newMember.token, newMember.node, newLag)
    }
  }

  it should "load the replication lag values from Zk on start" in new Builder {
    consistency.start()

    originalMapping.foreach { case (token, slaves) =>

      slaves.foreach { slave =>
        checkCachedAndPersistedLagValues(service, token, slave, 0)
      }
    }
  }

  it should "persist the new replication lag value in Zk when the value stays under the threshold" in new Builder {
    consistency.start()

    val token = 0
    val slave = originalMapping(token).head

    val newLag = 10

    // Update from 0s to 10s with a 60s threshold
    consistency.updateReplicationLagSeconds(token, slave, newLag)

    eventually {
      checkCachedAndPersistedLagValues(service, token, slave, newLag)
    }
  }

  it should "persist the new replication lag value in Zk when the value goes over the threshold" in new Builder {
    consistency.start()

    val token = 0
    val slave = originalMapping(token).head

    val newLag = 75

    // Update from 0s to 75s with a 60s threshold
    consistency.updateReplicationLagSeconds(token, slave, newLag)

    eventually {
      checkCachedAndPersistedLagValues(service, token, slave, newLag)
    }
  }

  it should "rate limit Zookeeper calls when the replication lag stays over the threshold" in new Builder {
    consistency.start()

    val token = 0
    val slave = originalMapping(token).head

    val initialLag = 300
    val firstUpdate = 150
    val secondUpdate = 75

    // Set the initial lag at 300
    // Won't rate limit because it is the first update
    consistency.updateReplicationLagSeconds(token, slave, initialLag)

    // Update from 300s to 150s with a 60s threshold
    // Should be rate limited
    consistency.updateReplicationLagSeconds(token, slave, firstUpdate)

    eventually {
      checkCachedAndPersistedLagValues(service, token, slave, initialLag)
    }

    // Advance time to get past threshold
    consistency.advanceTime(60000)

    // Update from 300s to 150s with a 60s threshold
    consistency.updateReplicationLagSeconds(token, slave, secondUpdate)

    eventually {
      checkCachedAndPersistedLagValues(service, token, slave, secondUpdate)
    }
  }

  it should "update the cached lag value when it changes in Zookeeper" in new Builder {
    consistency.start()

    val newLag = 12345

    val token = 0
    val slave = originalMapping(token).head

    // Update lag directly in Zookeeper
    zkUpdateReplicationLag(service, token, slave, newLag)

    eventually {
      checkCachedAndPersistedLagValues(service, token, slave, newLag)
    }
  }

  it should "change the master service member if the node provided is a slave on this shard" in new Builder {
    consistency.start()

    val token = 0

    val node = new Node("localhost", Map("nrv" -> 12346))
    val serviceMember = new ServiceMember(token, node)

    consistency.changeMasterServiceMember(token, node)

    zkGetServiceMember(service, token) should be(serviceMember)

    eventually {
      service.getMemberAtToken(token) should be(Some(serviceMember))
    }
  }

  it should "NOT change the master service member if the node provided is NOT a slave on this shard" in new Builder {
    consistency.start()

    val token = 0

    val node = new Node("localhost", Map("nrv" -> 12347))
    val serviceMember = new ServiceMember(token, node)

    intercept[IllegalArgumentException] {
      consistency.changeMasterServiceMember(token, node)
    }
  }
}