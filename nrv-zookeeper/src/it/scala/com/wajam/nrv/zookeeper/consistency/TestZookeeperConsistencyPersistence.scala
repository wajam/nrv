package com.wajam.nrv.zookeeper.consistency

import scala.concurrent.ExecutionContext.Implicits.global
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FlatSpec}
import org.scalatest.matchers.ShouldMatchers._
import akka.actor.ActorSystem
import com.wajam.nrv.service.{ServiceMember, Service}
import com.wajam.nrv.cluster.{Cluster, LocalNode, Node}
import com.wajam.nrv.zookeeper.{ZookeeperTestHelpers, ZookeeperClient}
import com.wajam.nrv.zookeeper.cluster.ZookeeperClusterManager


@RunWith(classOf[JUnitRunner])
class TestZookeeperConsistencyPersistence extends FlatSpec with BeforeAndAfter {

  implicit val ac = ActorSystem("TestZookeeperConsistencyPersistence")
  
  val TEST_PATH = "/tests/consistencypersistence"

  var zkClient: ZookeeperClient = _
  var cluster: Cluster = _

  before {
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

    val members = originalMapping.map { case (token, nodes) =>
      nodes.map { node =>
        new ServiceMember(token, node)
      }
    }.flatten

    val service = new Service("test.service")

    cluster.registerService(service)

    zkCreateService(service)

    originalMapping.map { case (token, nodes) =>
      zkCreateReplicasList(service, token, nodes)
    }

    members.foreach { member =>
      service.addMember(member, triggerEvent = false)
      zkCreateServiceMember(service, member)
    }

    cluster.start()

    val consistency = new ZookeeperConsistencyPersistence(zk, service)
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

    // Wait for the event to be triggered and the update to be complete
    Thread.sleep(100)

    consistency.explicitReplicasMapping should be(newMapping)
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

    // Wait for the event to be triggered and the update to be complete
    Thread.sleep(100)

    consistency.explicitReplicasMapping should be(newMapping)
  }

  it should "watch for the replicas when a service member is added" in new Builder {
    consistency.start()

    // Add a service member for token 20, without setting replicas yet
    val newMember = new ServiceMember(20, new Node("localhost", Map("nrv" -> 12349)))

    zkCreateServiceMember(service, newMember)

    // Trigger a NewMemberAddedEvent
    service.addMember(newMember, triggerEvent = true)

    // Wait for the event to be triggered and the update to be complete
    Thread.sleep(100)

    // At this point, the new token exists in the map but there are no replicas
    consistency.explicitReplicasMapping should be(originalMapping + (20 -> Nil))

    // Set the replicas list
    zkCreateReplicasList(service, 20, List(newMember.node))

    // Wait for the event to be triggered and the update to be complete
    Thread.sleep(100)

    // The replicas should now appear in the mapping
    consistency.explicitReplicasMapping should be(originalMapping + (20 -> List(newMember.node)))
  }
}