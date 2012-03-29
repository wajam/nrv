package com.wajam.nrv.protocol

import com.wajam.nrv.cluster.Cluster
import com.wajam.nrv.codec.NrvCodec

/**
 * Default protocol used by NRV. All nodes must have this protocol, since it's
 * used for cluster management.
 */
class NrvProtocol(cluster: Cluster) extends NettyProtocol("nrv", cluster, new NrvCodec()) {
}
