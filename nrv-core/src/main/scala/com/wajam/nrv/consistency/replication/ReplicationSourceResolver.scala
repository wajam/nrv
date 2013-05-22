package com.wajam.nrv.consistency.replication

import com.wajam.nrv.service._

/**
 * Special purpose resolver used to only resolve service member explicitly white listed as replication source.
 * This resolver is intended be used as the optional replicationResolver of ConsistencyMasterSlave and for routing
 * messages to the replication source.
 */
class ReplicationSourceResolver(sourceTokens: Seq[Long], resolver: => Resolver)
  extends Resolver(resolver.replica, resolver.tokenExtractor, resolver.constraints, resolver.sorter) {

  override def resolve(service: Service, token: Long) = {
    val endpoints = resolver.resolve(service, token)
    endpoints.shards.headOption match {
      case Some(shard) if sourceTokens.contains(shard.token) && shard.replicas.size > 0 => {
        val masterReplica = shard.replicas.head
        val localReplica = new Replica(token, service.cluster.localNode)
        new Endpoints(Seq(new Shard(token, Seq(masterReplica, localReplica))))
      }
      case _ => new Endpoints(Nil)
    }
  }
}
