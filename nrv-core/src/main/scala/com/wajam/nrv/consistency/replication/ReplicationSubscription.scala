package com.wajam.nrv.consistency.replication

import com.wajam.nrv.consistency.ResolvedServiceMember
import com.wajam.nrv.utils.timestamp.Timestamp

case class ReplicationSubscription(member: ResolvedServiceMember,
                                   cookie: String,
                                   mode: ReplicationMode,
                                   id: Option[String] = None,
                                   startTimestamp: Option[Timestamp] = None,
                                   endTimestamp: Option[Timestamp] = None)
