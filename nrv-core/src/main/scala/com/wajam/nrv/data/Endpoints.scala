package com.wajam.nrv.data

import com.wajam.nrv.cluster.Node


/**
 * Endpoints to which messages are sent, composed of logical endpoints (position in the ring, associated
 * to a token) and then for each logical, there are physical endpoints (actual node, composed of an ip and
 * nrv port).
 */
class Endpoints(val logicalEndpoints: Seq[LogicalEndpoint] = Seq()) extends Serializable {

  def onlinePhysicalEndpoints: Seq[PhysicalEndpoint] = logicalEndpoints.map(_.selectedEndpoints).flatten

  def noOnlinePhysicalEndpoints = onlinePhysicalEndpoints.size == 0
}

object Endpoints {
  val EMPTY = new Endpoints()
}

class LogicalEndpoint(val token: Long, val physicalEndpoints: Seq[PhysicalEndpoint]) extends Serializable {
  def selectedEndpoints: Seq[PhysicalEndpoint] = physicalEndpoints.filter(_.selected)
}

class PhysicalEndpoint(val token: Long, val node: Node, var selected: Boolean = true) extends Serializable
