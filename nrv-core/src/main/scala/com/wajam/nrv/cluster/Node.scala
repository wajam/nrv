package com.wajam.nrv.cluster

import java.net.{InetSocketAddress, InetAddress}
import com.wajam.nrv.InvalidParameter
import com.wajam.commons.InetUtils

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

  lazy val uniqueKey = "%s_%d".format(hostname, ports("nrv"))

  override def hashCode(): Int = uniqueKey.hashCode

  override def equals(that: Any) = that match {
    case other: Node => this.uniqueKey.equalsIgnoreCase(other.uniqueKey)
    case _ => false
  }

  def hostname  = fastGetHostName()
  def address = "%d.%d.%d.%d".format(host.getAddress: _*)

  /**
   * Special wrapper for address.getHostname
   *
   * Skip hostname resolution for host address since it will fail anyway (which is excepted and desirable), it will also
   * take non negligible time.
   */
  private def fastGetHostName() = {
    if (!Node.isTestAddress(host))
      host.getHostName
    else
      address
  }

  override def toString: String = "%s:%s".format(hostname, ports.map(t => "%s=%d".format(t._1, t._2)).mkString(","))
}

sealed class LocalNode(val listenAddress: InetAddress, ports: Map[String, Int])
  extends Node(Node.listenAddressToHostAddress(listenAddress), ports) {

  def this(listenAddress: String, ports: Map[String, Int]) = this(Node.addressByName(listenAddress), ports)
  def this(ports: Map[String, Int]) = this("0.0.0.0", ports)
}

object Node {

  /**
   * Every loopback address (127.0.0.0/8) outside the 127.0.0.0/16 range is reserved as a test address.
   *
   * This allow multiple test service on the same machine, providing they have different loopback ip.
   */
  def isTestAddress(address: InetAddress) = {
    val addr = address.getAddress

    address.isLoopbackAddress &&
    addr(0) == 127 &&
    addr(1) != 0 &&
    addr(2) != 0
  }

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

    // Do not resolve a test address, otherwise it will not be possible to bind more than one on the same host.
    if (address.isLoopbackAddress && !Node.isTestAddress(address)) InetUtils.firstInetAddress.getOrElse(address) else address
  }
}