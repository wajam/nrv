package com.wajam.nrv.cluster.zookeeper

import com.wajam.nrv.cluster.ClusterManager
import org.apache.zookeeper.ZooKeeper


/**
 * Zookeeper cluster manager
 */
class ZookeeperClusterManager extends ClusterManager {
  @volatile private var zk: ZooKeeper = null

  protected def initializeMembers() {}
}
