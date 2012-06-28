package com.wajam.nrv.cluster

import java.net.InetAddress

/**
 * Node (machine/process) that is member of a cluster and its services.
 */
class Node(val host: InetAddress, val ports: Map[String, Int]) extends Serializable {
  if (!ports.contains("nrv"))
    throw new UninitializedFieldError("Node must have at least a 'nrv' port defined")

  def this(host: String, ports: Map[String, Int]) = this(InetAddress.getByName(host), ports)

  def uniqueKey = "%s_%d".format(InetAddress.getLocalHost.getHostName.replace(".", "-"), ports("nrv"))

  override def hashCode():Int = uniqueKey.hashCode

  override def equals(that: Any) = that match {
    case other: Node => this.uniqueKey.equalsIgnoreCase(other.uniqueKey)
    case _ => false
  }
}
