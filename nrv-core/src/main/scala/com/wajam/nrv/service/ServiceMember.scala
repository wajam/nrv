package com.wajam.nrv.service

import com.wajam.nrv.cluster.Node

/**
 * Node that is member of a service, at a specific position (token) in
 * the consistent hashing ring of the service.
 */
class ServiceMember(var token: Long, var node: Node) extends Serializable {
}
