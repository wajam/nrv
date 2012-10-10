package com.wajam.nrv.cluster

/**
 * Cluster driver used to drive integration tests
 */
class TestingClusterDriver(var instanceCreator: (Int, Int, ClusterManager) => TestingClusterInstance) {
  var nodeInstances = Map[Node, TestingClusterInstance]()
  var instances = List[TestingClusterInstance]()
  var size: Int = 0

  private def init(size: Int) {
    // create instances
    this.size = size
    for (i <- 1 to size) {
      val instance = instanceCreator(size, i, new StaticClusterManager)
      this.nodeInstances += (instance.cluster.localNode -> instance)
      this.instances :+= instance
    }

    // add each node on each cluster instance
    for ((oNode, oInstance) <- this.nodeInstances) {
      for ((iNode, iInstance) <- this.nodeInstances) {
        if (oNode != iNode) {

          // copy all service members to all other services
          for ((oName, oService) <- oInstance.cluster.services) {
            val iService = iInstance.cluster.getService(oName)
            iService.ring.copyTo(oService.ring)
          }
        }
      }
    }

    // start each cluster instance
    for ((node, instance) <- this.nodeInstances) instance.cluster.start()
  }

  def destroy() {
    for (instance <- this.instances) instance.cluster.stop()
    this.nodeInstances = Map[Node, TestingClusterInstance]()
    this.instances = List[TestingClusterInstance]()
  }

  def execute(execute: (TestingClusterDriver, TestingClusterInstance) => Unit, fromSize: Int = 1, toSize: Int = 5) {
    for (i <- fromSize to toSize) {
      this.destroy()
      this.init(i)
      execute(this, this.instances(0))
    }
  }
}

class TestingClusterInstance(var cluster: Cluster, var data: AnyRef = null) {
}

