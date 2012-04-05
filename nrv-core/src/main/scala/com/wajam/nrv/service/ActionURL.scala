package com.wajam.nrv.service

/**
 * URL used in NRV to represent an action (path) within a service (host) that can
 * be reached through a protocol
 */
class ActionURL(var service:String, var path:ActionPath = "/", var protocol:String = "nrv") {

  override def toString = "%s://%s%s".format(protocol, service, path)
}
