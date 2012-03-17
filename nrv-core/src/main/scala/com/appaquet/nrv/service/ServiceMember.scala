package com.appaquet.nrv.service

import com.appaquet.nrv.cluster.Node

/**
 * Node that is member of a service, at a specific position (token) in
 * the consistent hashing ring of the service.
 */
class ServiceMember(var token: Long, var node: Node) {

}
