package com.wajam.nrv.cluster.zookeeper

import com.wajam.nrv.cluster._
import com.wajam.nrv.service.{MemberStatus, ServiceMember, Service}
import org.apache.zookeeper.CreateMode
import com.wajam.nrv.cluster.zookeeper.ZookeeperClient._

/**
 * Cluster driver used to drive integration tests
 */
class ZookeeperTestingClusterDriver(var instanceCreator: (Int, Int, ClusterManager) => TestingClusterInstance) {
  var instances = List[TestingClusterInstance]()
  var zkClients = List[ZookeeperClient]()

  private def init(size: Int) {
    // create instances
    for (i <- 1 to size) {

      val zkClient = new ZookeeperClient("127.0.0.1/tests/clustermanager", autoConnect = false)
      val instance = instanceCreator(size, i, new ZookeeperClusterManager(zkClient))

      // Create minimal service structure in zookeeper
      for (service <- instance.cluster.services.values) zkCreateService(service)

      zkClient.connect()
      instance.cluster.start()
      this.instances :+= instance
      this.zkClients :+= zkClient
    }

    // Wait until all service members are up
    waitForCondition[Boolean](
      instances.flatMap(_.cluster.services.values.flatMap(_.members)).forall(_.status == MemberStatus.Up), _ == true)
  }

  def destroy() {
    for (instance <- this.instances) instance.cluster.stop()
    this.instances = List[TestingClusterInstance]()

    for (zkClient <- this.zkClients) zkClient.close()
    this.zkClients = List[ZookeeperClient]()
  }

  def execute(execute: (ZookeeperTestingClusterDriver, TestingClusterInstance) => Unit, fromSize: Int = 1, toSize: Int = 5) {
    this.init(toSize)
    for (i <- fromSize to toSize) {
      // TODO: execute on a random instance
      execute(this, this.instances(0))
    }
  }

  def zkCreateService(service: Service) {
    val zkClient = new ZookeeperClient("127.0.0.1/tests/clustermanager")
    val path = ZookeeperClusterManager.zkServicePath(service.name)
    zkClient.ensureExists(path, service.name, CreateMode.PERSISTENT)

    for (member <- service.members) {
      zkCreateServiceMember(zkClient, service, member)
    }
    zkClient.close()
  }

  def zkCreateServiceMember(zkClient: ZookeeperClient, service: Service, serviceMember: ServiceMember) {
    val path = ZookeeperClusterManager.zkMemberPath(service.name, serviceMember.token)
    val created = zkClient.ensureExists(path, serviceMember.toString, CreateMode.PERSISTENT)

    // if node existed, overwrite
    if (created)
      zkClient.set(path, serviceMember.toString)
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
    zk.ensureExists("/tests", "")
    zk.ensureExists("/tests/clustermanager", "")
    zk.close()
  }
}
