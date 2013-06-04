package com.wajam.nrv.consistency.replication

import com.wajam.nrv.service._
import scala.Some
import com.wajam.nrv.cluster.Node

class ExplicitReplicaResolver(explicitTokenMapping: Map[Long,List[Node]], resolver: => Resolver)
  extends Resolver(resolver.replica, resolver.tokenExtractor, resolver.constraints, resolver.sorter) {

  override def resolve(service: Service, token: Long) = {
    val memberNodes: List[Node] = service.resolveMembers(token, 1).headOption match {
      case Some(member) => member.node :: explicitTokenMapping.get(token).filterNot(_ == member.node)
      case _ => explicitTokenMapping.get(token).getOrElse(List())
    }

    //disable nodes with status == down
    // TODO: implement multiple shards
    val shards = new Shard(token, memberNodes.flatMap(node => service.members.find(_.node == node)).map(member => {
      new Replica(member.token, member.node, selected = member.status == MemberStatus.Up)
    }))
    new Endpoints(Seq(shards))
  }
}