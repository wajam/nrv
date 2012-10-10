package com.wajam.nrv.cluster

import actors.Actor
import com.wajam.nrv.utils.Scheduled

/**
 * Manager of a cluster in which nodes can be added/removed and can goes up and down.
 */
abstract class DynamicClusterManager extends ClusterManager {

  private val CLUSTER_CHECK_IN_MS = 1000

  override def start() {
    super.start()
    EventLoop.start()
  }

  object EventLoop extends Actor with Scheduled {

    case object CheckCluster

    override def start(): Actor = {
      this.scheduleMessage(CheckCluster, CLUSTER_CHECK_IN_MS, CLUSTER_CHECK_IN_MS)
      super.start()
    }

    def act() {
      loop {
        react {
          case CheckCluster =>

        }
      }
    }
  }

}
