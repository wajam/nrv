package com.wajam.nrv.service

import com.wajam.nrv.protocol.Protocol
import com.wajam.nrv.cluster.Node

/**
 * Service handled by the cluster that offers actions that can be executed on remote nodes. A service
 * can be seen as a domain in an URL (mail.google.com), path are actions (/message/232323/). Nodes of
 * the cluster all handles a subset of path calls, distributed using the resolver that spread calls
 * over service members (nodes)
 *
 * Members of the service are represented by a consistent hashing ring (@see Ring)
 */
class Service(var name: String, protocol: Option[Protocol] = None, resolver: Option[Resolver] = None) extends ActionSupport {
  var ring = new Object with Ring[Node]
  var actions = List[Action]()

  applySupport(service = Some(this), protocol = protocol, resolver = resolver)

  def addMember(token: Long, node: Node) = this.ring.add(token, node)

  def resolveMembers(token: Long, count: Int) = this.ring.resolve(token, count)

  def membersCount = this.ring.size

  protected[nrv] def start() {
    for (action <- this.actions) {
      action.start()
    }
  }

  protected[nrv] def stop() {
    for (action <- this.actions) {
      action.stop()
    }
  }

  def registerAction(action: Action): Action = {
    action.supportedBy(this)

    this.actions ::= action
    action
  }

  def findAction(path: ActionPath, method: String): Option[Action] = {
    this.actions find { _.matches(path, method) }
  }
}
