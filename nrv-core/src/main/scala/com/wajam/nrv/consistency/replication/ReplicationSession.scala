package com.wajam.nrv.consistency.replication

import com.wajam.nrv.consistency.ResolvedServiceMember
import com.wajam.nrv.utils.timestamp.Timestamp
import com.wajam.nrv.cluster.Node

case class ReplicationSession(member: ResolvedServiceMember,
                              cookie: String,
                              mode: ReplicationMode,
                              slave: Node,
                              id: Option[String] = None,
                              startTimestamp: Option[Timestamp] = None,
                              endTimestamp: Option[Timestamp] = None,
                              secondsBehindMaster: Option[Int] = None)
