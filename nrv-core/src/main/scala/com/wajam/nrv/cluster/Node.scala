package com.wajam.nrv.cluster

import java.net.{InetSocketAddress, InetAddress}
import com.wajam.nrv.InvalidParameter
import com.wajam.nrv.utils.InetUtils

/**
 * Node (machine/process) that is member of a cluster and its services.
 * @param host Host of the node
 * @param ports Ports of each protocol running on the node
 */
sealed class Node(val host: InetAddress, val ports: Map[String, Int]) extends Serializable {
  def this(host: String, ports: Map[String, Int]) = this(Node.addressByName(host), ports)

  if (!ports.contains("nrv"))
    throw new UninitializedFieldError("Node must have at least a 'nrv' port defined")

  if (host.isAnyLocalAddress)
    throw new InvalidParameter("Node host must be one of the local address")

  val protocolsSocketAddress: Map[String, InetSocketAddress] = ports.map(tup => (tup._1 -> new InetSocketAddress(host, tup._2)))

  lazy val uniqueKey = "%s_%d".format(host.getHostName, ports("nrv"))

  override def hashCode(): Int = uniqueKey.hashCode

  override def equals(that: Any) = that match {
    case other: Node => this.uniqueKey.equalsIgnoreCase(other.uniqueKey)
    case _ => false
  }

  override def toString: String = "%s:%s".format(host.getHostName, ports.map(t => "%s=%d".format(t._1, t._2)).mkString(","))
}

sealed class LocalNode(val listenAddress: InetAddress, ports: Map[String, Int])
  extends Node(Node.listenAddressToHostAddress(listenAddress), ports) {

  def this(listenAddress: String, ports: Map[String, Int]) = this(Node.addressByName(listenAddress), ports)
  def this(ports: Map[String, Int]) = this("0.0.0.0", ports)
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

  def listenAddressToHostAddress(listenAddress: InetAddress): InetAddress = {
    if (listenAddress.isAnyLocalAddress) InetUtils.firstInetAddress.getOrElse(listenAddress) else listenAddress
  }

  def addressByName(name: String): InetAddress = {
    val address = InetAddress.getByName(name)
    if (mustResolveLocalAddress(address)) InetUtils.firstInetAddress.getOrElse(address) else address
  }

  /**
   * Force resolve only of the classic localhost or 127.0.0.1, but don't resolve
   * every other 127.0.0.1/8 addresses. This allow multiple loopback for testing.
   */
  def mustResolveLocalAddress(address: InetAddress) = {
    address.getAddress == Array[Byte](127,0,0,1) ||
    address.getHostName == "localhost"
  }
}