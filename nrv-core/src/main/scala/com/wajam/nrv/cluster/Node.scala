package com.wajam.nrv.cluster

import java.net.InetAddress

/**
 * Node (machine/process) that is member of a cluster and its services.
 */
sealed class Node(val host: InetAddress, val ports: Map[String, Int]) extends Serializable {
  if (!ports.contains("nrv"))
    throw new UninitializedFieldError("Node must have at least a 'nrv' port defined")

  def this(host: String, ports: Map[String, Int]) = this(InetAddress.getByName(host), ports)

  def uniqueKey = "%s_%d".format(host.getHostName, ports("nrv"))

  override def hashCode(): Int = uniqueKey.hashCode

  override def equals(that: Any) = that match {
    case other: Node => this.uniqueKey.equalsIgnoreCase(other.uniqueKey)
    case _ => false
  }

  override def toString: String = "%s:%s".format(host.getHostName, ports.map(t => "%s=%d".format(t._1, t._2)).mkString(","))
}

object Node {

  def fromString(nodeString: String): Node = {
    val Array(strHost, strPorts) = nodeString.split(":")

    var mapPorts = Map[String, Int]()
    for (strSrvPort <- strPorts.split(",")) {
      val Array(strService, strPort) = strSrvPort.split("=")
      mapPorts += (strService -> strPort.toInt)
    }

    new Node(strHost, mapPorts)
  }

}