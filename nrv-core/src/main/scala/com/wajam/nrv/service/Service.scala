package com.wajam.nrv.service

import com.wajam.nrv.protocol.Protocol
import com.wajam.nrv.utils.Observable
import com.wajam.nrv.consistency.Consistency
import com.wajam.nrv.cluster.Node

/**
 * Service handled by the cluster that offers actions that can be executed on remote nodes. A service
 * can be seen as a domain in an URL (mail.google.com), path are actions (/message/232323/). Nodes of
 * the cluster all handles a subset of path calls, distributed using the resolver that spread calls
 * over service members (nodes)
 *
 * Members of the service are represented by a consistent hashing ring (@see Ring)
 */
class Service(val name: String,
              defaultProtocol: Option[Protocol] = None,
              defaultResolver: Option[Resolver] = None,
              defaultConsistency: Option[Consistency] = None)
  extends ActionSupport with Observable {

  val ring = new Object with Ring[ServiceMember]
  var actions = List[Action]()

  // override protocol, resolver if defined
  applySupport(service = Some(this), protocol = defaultProtocol, resolver = defaultResolver, consistency = defaultConsistency)

  override def toString: String = name

  def members: Iterable[ServiceMember] = this.ring.nodes.map(node => node.element)

  def membersCount = this.ring.size

  def getMemberAtToken(token: Long): Option[ServiceMember] = this.ring.find(token) match {
    case None => None
    case Some(ringNode) => Some(ringNode.element)
  }

  def addMember(token: Long, node: Node): ServiceMember = addMember(new ServiceMember(token, node))

  def addMember(member: ServiceMember): ServiceMember = {
    member.addParentObserver(this)
    this.ring.add(member.token, member)
    member
  }

  lazy val maximumReplica: Int = actions.foldLeft(0)((cur, action) =>
    if (action.resolver.replica > cur) action.resolver.replica else cur
  )

  def resolveMembers(token: Long, count: Int): Seq[ServiceMember] = this.ring.resolve(token, count).map(node => node.element)

  def resolveMembers(token: Long, count: Int, filter: ServiceMember => Boolean): Seq[ServiceMember] = {
    this.ring.resolve(token, count, node => {
      filter(node.element)
    }).map(node => node.element)
  }

  def start() {
    for (action <- this.actions) {
      action.start()
    }
  }

  def stop() {
    for (action <- this.actions) {
      action.stop()
    }
  }

  def registerAction(action: Action): Action = {
    action.supportedBy(this)

    this.actions ::= action
    action
  }

  def registerActions(actions: List[Action]): List[Action] = {
    actions.foreach(registerAction(_))
    actions
  }

  def findAction(path: ActionPath, method: String): Option[Action] = {
    this.actions find {
      _.matches(path, method)
    }
  }

  def printService: String = {
    (Seq(
      "Service: %s".format(name),
      "Token\tNode\t\t\t\t\tStatus"
    ) ++
      members.map(member => "%d\t\t%s\t\t%s".format(member.token, member.node, member.status))
      ).mkString("\n")
  }
}
