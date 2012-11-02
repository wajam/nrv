package com.wajam.nrv.cluster.zookeeper

import com.wajam.nrv.cluster._
import com.wajam.nrv.service.{ServiceMember, Service}
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
  }

  def destroy() {
    for (instance <- this.instances) instance.cluster.stop()
    this.instances = List[TestingClusterInstance]()

    for (zkClient <- this.zkClients) zkClient.close()
    this.zkClients = List[ZookeeperClient]()
  }

  def execute(execute: (ZookeeperTestingClusterDriver, TestingClusterInstance) => Unit, fromSize: Int = 1, toSize: Int = 5) {
    this.init(toSize)

    // TEMPORARY: hard coded sleep to ensure all nodes on clusters are up.
    Thread.sleep(5000)

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
