package com.wajam.nrv.cluster.zookeeper

import com.wajam.nrv.cluster.ClusterManager
import org.apache.zookeeper.ZooKeeper


/**
 * Zookeeper cluster manager
 */
class ZookeeperClusterManager extends ClusterManager {
  @volatile private var zk: ZooKeeper = null

  def start() {
    // TODO: Do something with zookeeper
  }

  def stop() {}
}
