package com.wajam.nrv.protocol

import java.nio.ByteBuffer
import com.wajam.nrv.data.Message
import com.wajam.nrv.transport.nrv.NrvNettyTransport
import com.wajam.nrv.cluster.LocalNode
import com.wajam.nrv.data.serialization.NrvProtobufSerializer
import com.google.common.primitives.UnsignedBytes

/**
 * Default protocol used by NRV. All nodes must have this protocol, since it's
 * used for cluster management.
 */
class NrvProtocol(localNode: LocalNode,
                  idleConnectionTimeoutMs: Long,
                  maxConnectionPoolSize: Int)
  extends NrvLocalOptimizedTransport("nrv",
                                     localNode,
                                     new NrvMemoryProtocol("nry-local", localNode),
                                     new NrvBinaryProtocol("nry-binary", localNode,
                                                           idleConnectionTimeoutMs,
                                                           maxConnectionPoolSize)) {


}


