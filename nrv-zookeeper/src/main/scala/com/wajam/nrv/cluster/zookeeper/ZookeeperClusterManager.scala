package com.wajam.nrv.cluster.zookeeper

import com.wajam.nrv.cluster.ClusterManager
import org.apache.zookeeper.ZooKeeper

/**
 * DESCRIPTION HERE
 *
 * Author: Andre-Philippe Paquet <andre-philippe@wajam.com>
 * Copyright (c) Wajam
 */

class ZookeeperClusterManager extends ClusterManager {
  @volatile private var zk: ZooKeeper = null
}
