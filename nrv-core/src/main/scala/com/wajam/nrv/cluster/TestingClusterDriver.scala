package com.wajam.nrv.cluster

/**
 * Cluster driver used to drive integration tests
 */
class TestingClusterDriver(var clusterCreator: (Int, ClusterManager) => Cluster) extends ClusterManager {
  var clusters = Map()

  def init() {
  }

  def destroy() {
  }

  def test(exec: Cluster => Unit, fromSize:Int = 1, toSize:Int = 5) {
  }

}

object TestingClusterDriver {
  val driver = new TestingClusterDriver((i, manager) => {
    new Cluster(new Node("127.0.0.1", Map("nrv" -> (5000+i))), manager)
  })
}
