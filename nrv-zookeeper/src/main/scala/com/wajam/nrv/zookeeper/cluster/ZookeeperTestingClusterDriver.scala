package com.wajam.nrv.zookeeper.cluster

import com.wajam.nrv.cluster._
import com.wajam.nrv.service.{MemberStatus, ServiceMember, Service}
import org.apache.zookeeper.CreateMode
import com.wajam.nrv.zookeeper.ZookeeperClient._
import com.wajam.nrv.zookeeper.ZookeeperClient

/**
 * Cluster driver used to drive integration tests
 */
class ZookeeperTestingClusterDriver(var instanceCreator: (Int, Int, ZookeeperClusterManager) => TestingClusterInstance) {
  var instances = List[ZookeeperTestingClusterInstance]()

  def init(size: Int) {

    // Create instances
    for (i <- 1 to size) {

      val zkClient = new ZookeeperClient("127.0.0.1/tests/clustermanager", autoConnect = false)
      val instance = instanceCreator(size, i, new ZookeeperClusterManager(zkClient))

      // Create minimal service structure in zookeeper
      for (service <- instance.cluster.services.values) zkCreateService(service)

      this.instances :+= ZookeeperTestingClusterInstance(zkClient, instance)
    }

    // Start instances
    for (instance <- instances) {
      instance.zkClient.connect()
      instance.cluster.start()
    }

    // Wait until all service members are up
    waitForCondition[Boolean](
      instances.flatMap(_.cluster.services.values.flatMap(_.members)).forall(_.status == MemberStatus.Up), _ == true)
  }

  def destroy() {
    for (instance <- instances) {
      instance.cluster.stop()
      instance.zkClient.close()
    }
    instances = List[ZookeeperTestingClusterInstance]()
  }

  def execute(execute: (ZookeeperTestingClusterDriver, TestingClusterInstance) => Unit, size: Int = 5) {
    this.init(size)
    for (i <- 0 until size) {
      execute(this, this.instances(i))
    }
  }

  def zkCreateService(service: Service) {
    val zkClient = new ZookeeperClient("127.0.0.1/tests/clustermanager")
    val path = ZookeeperClusterManager.zkServicePath(service.name)
    zkClient.ensureAllExists(path, service.name, CreateMode.PERSISTENT)

    for (member <- service.members) {
      zkCreateServiceMember(zkClient, service, member)
    }
    zkClient.close()
  }

  def zkCreateServiceMember(zkClient: ZookeeperClient, service: Service, serviceMember: ServiceMember) {
    val path = ZookeeperClusterManager.zkMemberPath(service.name, serviceMember.token)
    val created = zkClient.ensureAllExists(path, serviceMember.toString, CreateMode.PERSISTENT)

    // if node existed, overwrite
    if (created)
      zkClient.set(path, serviceMember.toString)

    val votePath = ZookeeperClusterManager.zkMemberVotesPath(service.name, serviceMember.token)
    zkClient.ensureAllExists(votePath, "", CreateMode.PERSISTENT)
  }

  def waitForCondition[T](block: => T, condition: (T) => Boolean, sleepTimeInMs: Long = 250, timeoutInMs: Long = 10000) {
    val startTime = System.currentTimeMillis()
    while (!condition(block)) {
      if ((System.currentTimeMillis() - startTime) > timeoutInMs) {
        throw new RuntimeException("Timeout waiting for condition.")
      }
      Thread.sleep(sleepTimeInMs)
    }
  }
}

object ZookeeperTestingClusterDriver {

  def cleanupZookeeper() {
    val zk = new ZookeeperClient("127.0.0.1")
    try {
      zk.deleteRecursive("/tests/clustermanager")
    } catch {
      case e: Exception =>
    }
    zk.ensureAllExists("/tests/clustermanager", "")
    zk.close()
  }
}

class ZookeeperTestingClusterInstance(val zkClient: ZookeeperClient, cluster: Cluster, data: AnyRef = null)
  extends TestingClusterInstance(cluster, data) {

  def isUp: Boolean = {
    cluster.services.values.flatMap(_.members.filter(_.node == cluster.localNode)).forall(_.status == MemberStatus.Up)
  }
}

object ZookeeperTestingClusterInstance {
  def apply(zkClient: ZookeeperClient, cluster: Cluster, data: AnyRef = null) =
    new ZookeeperTestingClusterInstance(zkClient, cluster, data)

  def apply(zkClient: ZookeeperClient, instance: TestingClusterInstance) = {
    instance match {
      case zkInstance: ZookeeperTestingClusterInstance => zkInstance
      case _ => new ZookeeperTestingClusterInstance(zkClient, instance.cluster, instance.data)
    }
  }
}
