package com.appaquet.nrv.service

import com.appaquet.nrv.protocol.Protocol
import com.appaquet.nrv.cluster.Node

/**
 * Service handled by the cluster that offers actions that can be executed on remote nodes. A service
 * can be seen as a domain in an URL (mail.google.com), path are actions (/message/232323/). Nodes of
 * the cluster all handles a subset of path calls, distributed using the resolver that spread calls
 * over service members (nodes)
 *
 * Members of the service are represented by a consistent hashing ring (@see Ring)
 */
class Service(var name: String, protocol: Option[Protocol] = None, resolver: Option[Resolver] = None) extends ActionSupport with Ring[Node] {
  var actions = List[Action]()

  applySupport(service = Some(this), protocol = protocol, resolver = resolver)

  def bind(path: String, action: Action): Action = {
    action.supportedBy(this)
    action.path = path
    this.actions ::= action

    action
  }

  def findAction(path: String): Option[Action] = {
    this.actions find {
      _.matches(path)
    }
  }
}
