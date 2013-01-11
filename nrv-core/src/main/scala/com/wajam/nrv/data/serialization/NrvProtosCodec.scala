package com.wajam.nrv.data.serialization

import scala.collection.JavaConverters._
import com.wajam.nrv.data.Message
import com.google.protobuf.ByteString
import com.wajam.nrv.cluster.Node
import com.wajam.nrv.service.{Replica, Shard, Endpoints}
import com.wajam.nrv.protocol.codec.{Codec, GenericJavaSerializeCodec}
import java.net.InetAddress
import com.wajam.nrv.data.serialization.NrvProtos.Int32Pair
import collection.parallel.mutable

/**
 * Convert NRV principal objects to their Protobuf equivalent back and forth
 */
class NrvProtosCodec {

  val javaSerialize = new GenericJavaSerializeCodec()

  def encodeMessage(message: Message, messageDataCodec: Codec): NrvProtos.Message = {

    val protoMessage = NrvProtos.Message.newBuilder()

    protoMessage.setProtocolName(message.protocolName)
    protoMessage.setServiceName(message.serviceName)

    protoMessage.setMethod(message.method)

    protoMessage.setPath(message.path)
    protoMessage.setRendezVousId(message.rendezvousId)

    protoMessage.setError(ByteString.copyFrom(serializeToBytes(message.error)))

    protoMessage.setFunction(message.function)

    protoMessage.setSource(encodeNode(message.source))

    protoMessage.setDestination(encodeEndpoints(message.destination))

    protoMessage.setToken(message.token)

    message.parameters.foreach{
      case (key, value) =>
        protoMessage.addParameters(NrvProtos.StringPair.newBuilder().setKey(key).setValue(value.toString))}

    message.metadata.foreach{
      case (key, value) =>
        protoMessage.addMetadata(NrvProtos.StringPair.newBuilder().setKey(key).setValue(value.toString))}

    protoMessage.setMessageData(ByteString.copyFrom(messageDataCodec.encode(message.messageData)))

    protoMessage.build()
  }

  def decodeMessage(data: Array[Byte]): Message = {
    sys.error("unimplemented")
  }

  def encodeNode(node: Node): NrvProtos.Node = {
    val protoNode = NrvProtos.Node.newBuilder()

    protoNode.setHost(ByteString.copyFrom(node.host.getAddress))

    node.ports.foreach{
      case (key, value) =>
        protoNode.addPorts(NrvProtos.Int32Pair.newBuilder().setKey(key).setValue(value))}

    protoNode.build()
  }

  def decodeNode(protoNode: NrvProtos.Node): Node = {
    val portList = protoNode.getPortsList.asScala.toList
    val portMap = for(p <- portList) yield ((p.getKey, p.getValue))
    new Node(InetAddress.getByAddress(protoNode.getHost().toByteArray), portMap.toMap)
  }

  def encodeEndpoints(endpoints: Endpoints): NrvProtos.Endpoints = {
    val protoEndpoint = NrvProtos.Endpoints.newBuilder()

    endpoints.shards.foreach{
      case (shard) =>
        protoEndpoint.addShards(encodeShard(shard))}

    protoEndpoint.build()
  }

  def decodeEndpoints(node: NrvProtos.Endpoints): Endpoints = {
    sys.error("unimplemented")
  }

  def encodeShard(shard: Shard): NrvProtos.Endpoints.Shard = {
    sys.error("unimplemented")
  }

  def decodeShard(shard: NrvProtos.Endpoints.Shard) = {
    sys.error("unimplemented")
  }

  def encodeReplica(replica: Replica): NrvProtos.Endpoints.Replica = {
    val proto = NrvProtos.Endpoints.Replica.newBuilder()

    proto.setToken(replica.token)
    proto.setSelected(replica.selected)
    proto.setNode(encodeNode(replica.node))

    proto.build()
  }

  def decodeReplica(proto: NrvProtos.Endpoints.Replica): Replica = {

    new Replica(proto.getToken, decodeNode(proto.getNode), proto.getSelected)
  }

  def encodeUsingCodec(messageData: Any): Array[Byte] = {
    sys.error("unimplemented")
  }

  def decodeUsingCodec(messageRawData: Array[Byte]): Any = {
    sys.error("unimplemented")
  }

  def serializeToBytes(entity: AnyRef): Array[Byte] = {
    javaSerialize.encodeAny(entity)
  }

  def serializeFromBytes(bytes: Array[Byte]) = {
    javaSerialize.decodeAny(bytes)
  }
}

