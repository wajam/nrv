package com.appaquet.nrv.cluster

import java.net.InetAddress

/**
 * Node (machine/process) that is member of a cluster and its services.
 */
class Node(val host:InetAddress, val ports:Map[String, Int]) extends Serializable {
  if (!ports.contains("nrv"))
    throw new UninitializedFieldError("Node must have a 'nrv' port defined")

  def this(host:String, ports:Map[String,  Int]) = this(InetAddress.getByName(host), ports)
}
