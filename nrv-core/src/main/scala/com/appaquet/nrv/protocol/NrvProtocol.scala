package com.appaquet.nrv.protocol

import com.appaquet.nrv.cluster.Cluster
import com.appaquet.nrv.codec.NrvCodec

/**
 * Default protocol used by NRV. All nodes must have this protocol, since it's
 * used for cluster management.
 */
class NrvProtocol(cluster: Cluster) extends NettyProtocol("nrv", cluster, new NrvCodec()) {
}
