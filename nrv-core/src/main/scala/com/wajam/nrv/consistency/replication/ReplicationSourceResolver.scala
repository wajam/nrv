package com.wajam.nrv.consistency.replication

import com.wajam.nrv.service._

/**
 * Special purpose resolver used to explicitly specify replication sources. Specify a list of service member which
 * must be used as replication publisher source for the current node (see ServiceMember.fromString() for the string
 * format). This resolver is intended be used as the optional replicationResolver of ConsistencyMasterSlave and
 * for routing messages to the replication source.
 */
class ReplicationSourceResolver(sourceReplicas: Seq[String], resolver: => Resolver)
  extends Resolver(resolver.replica, resolver.tokenExtractor, resolver.constraints, resolver.sorter) {

  private val sourceMembers = sourceReplicas.map(s => ServiceMember.fromString(s))

  override def resolve(service: Service, token: Long) = {
    // Resolve the specified token to find the serving service member token. If the resolved token one of the
    // configured replication source, use that source as primary replica and this node as secondary replica.
    resolver.resolve(service, token).shards.headOption match {
      case Some(shard) => {
        sourceMembers.collectFirst({case member if member.token == shard.token => member}) match {
          case Some(sourceMember) => {
            val sourceReplica = new Replica(token, sourceMember.node)
            val thisReplica = new Replica(token, service.cluster.localNode)
            new Endpoints(Seq(new Shard(token, Seq(sourceReplica, thisReplica))))
          }
          case None => new Endpoints(Nil)
        }
      }
      case None => new Endpoints(Nil)
    }
  }
}
