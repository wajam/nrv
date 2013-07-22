package com.wajam.nrv.service

import com.wajam.nrv.service._
import scala.Some
import com.wajam.nrv.cluster.Node

/**
 * This resolver is also commonly referred to as "explicit mapping resolver". As opposed to the natural ring resolving
 * which maps every shards to a single master node and replicas to the following nodes, this resolver relies solely
 * on the specified explicit configuration. The master of the shard is resolved through nrv mechanics and added as
 * the head of the resolved node list. All other nodes are considered to be replicas.
 */
class ExplicitReplicaResolver(explicitTokenMapping: Map[Long,List[Node]], resolver: Resolver)
  extends Resolver(resolver.replica, resolver.tokenExtractor, resolver.constraints, resolver.sorter) {

  override def resolve(service: Service, token: Long) = {

    val memberReplica: List[Replica] = service.resolveMembers(token, 1).headOption match {
      case Some(member) => {
        new Replica(token,member.node,member.status == MemberStatus.Up) :: explicitTokenMapping.get(token).getOrElse(List()).filterNot(_ == member.node).map(new Replica(token, _))
      }
      case _ => explicitTokenMapping.get(token).getOrElse(List()).map(new Replica(token, _))
    }

    new Endpoints(Seq(new Shard(token, memberReplica)))
  }
}