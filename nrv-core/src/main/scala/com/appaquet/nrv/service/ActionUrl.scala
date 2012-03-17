package com.appaquet.nrv.service

/**
 * URL used in NRV to represent an action (path) within a service (host) that can
 * be reached through a protocol
 */
class ActionUrl(var service: String, var path: String, var protocol: String = "nrv") {
  // TODO: Should implement java URL

  override def toString = String.format("%s://%s%s", protocol, service, path)
}
