package com.wajam.nrv.data.serialization

import scala.collection.JavaConverters._
import com.wajam.nrv.data.{SerializableMessage, Message}
import com.google.protobuf.ByteString
import com.wajam.nrv.cluster.Node
import com.wajam.nrv.service.{Replica, Shard, Endpoints}
import com.wajam.nrv.protocol.codec.{Codec, GenericJavaSerializeCodec}
import java.net.InetAddress

/**
 * Convert NRV principal objects to their Protobuf equivalent back and forth
 */
class NrvProtobufSerializer {

  val javaSerialize = new GenericJavaSerializeCodec()

  def encodeMessage(message: Message, messageDataCodec: Codec): NrvProtobuf.Message = {

    val protoMessage = NrvProtobuf.Message.newBuilder()

    protoMessage.setCode(message.code)

    protoMessage.setProtocolName(message.protocolName)
    protoMessage.setServiceName(message.serviceName)

    protoMessage.setMethod(message.method)

    protoMessage.setPath(message.path)
    protoMessage.setRendezVousId(message.rendezvousId)

    if (message.error.isDefined)
      protoMessage.setError(ByteString.copyFrom(serializeToBytes(message.error.get)))

    protoMessage.setFunction(message.function)

    protoMessage.setSource(encodeNode(message.source))

    protoMessage.setDestination(encodeEndpoints(message.destination))

    protoMessage.setToken(message.token)

    message.parameters.foreach {
      case (key, value) =>
        protoMessage.addParameters(NrvProtobuf.StringPair.newBuilder().setKey(key).setValue(value.asInstanceOf[String]))
    }

    message.metadata.foreach {
      case (key, value) =>
        protoMessage.addMetadata(NrvProtobuf.StringPair.newBuilder().setKey(key).setValue(value.asInstanceOf[String]))
    }

    protoMessage.setMessageData(ByteString.copyFrom(messageDataCodec.encode(message.messageData)))

    protoMessage.build()
  }

  def decodeMessage(protoMessage: NrvProtobuf.Message, messageDataCodec: Codec): Message = {
    val parameters = for (p <- protoMessage.getParametersList.asScala.toList) yield ((p.getKey, p.getValue))
    val metadata = for (p <- protoMessage.getMetadataList.asScala.toList) yield ((p.getKey, p.getValue))
    val messageData = messageDataCodec.decode(protoMessage.getMessageData.toByteArray)

    val destination = decodeEndpoints(protoMessage.getDestination)

    val message = new SerializableMessage(parameters, metadata, messageData)

    message.code = protoMessage.getCode

    message.protocolName = protoMessage.getProtocolName
    message.serviceName = protoMessage.getProtocolName
    message.method = protoMessage.getMethod
    message.path = protoMessage.getPath

    //TODO: Modify message.rendezvousId when we won't use JavaSerialization anymore to use a long
    message.rendezvousId = protoMessage.getRendezVousId.asInstanceOf[Int]

    val error = protoMessage.getError

    if (error.size() != 0)
      message.error = Some(serializeFromBytes(protoMessage.getError.toByteArray).asInstanceOf[Exception])

    message.function = protoMessage.getFunction

    message.source = decodeNode(protoMessage.getSource)
    message.destination = destination
    message.token = protoMessage.getToken

    message
  }

  def encodeNode(node: Node): NrvProtobuf.Node = {
    val protoNode = NrvProtobuf.Node.newBuilder()

    protoNode.setHost(ByteString.copyFrom(node.host.getAddress))

    node.ports.foreach {
      case (key, value) =>
        protoNode.addPorts(NrvProtobuf.Int32Pair.newBuilder().setKey(key).setValue(value))
    }

    protoNode.build()
  }

  def decodeNode(protoNode: NrvProtobuf.Node): Node = {
    val portList = protoNode.getPortsList.asScala.toSeq
    val portMap = for (p <- portList) yield ((p.getKey, p.getValue))
    new Node(InetAddress.getByAddress(protoNode.getHost.toByteArray), portMap.toMap)
  }

  def encodeEndpoints(endpoints: Endpoints): NrvProtobuf.Endpoints = {
    val protoEndpoint = NrvProtobuf.Endpoints.newBuilder()

    endpoints.shards.foreach {
      case (shard) =>
        protoEndpoint.addShards(encodeShard(shard))
    }

    protoEndpoint.build()
  }

  def decodeEndpoints(protoEndpoints: NrvProtobuf.Endpoints): Endpoints = {
    val protoShardSeq = protoEndpoints.getShardsList.asScala.toSeq
    val shardSeq = for (shard <- protoShardSeq) yield (decodeShard(shard))
    new Endpoints(shardSeq)
  }

  def encodeShard(shard: Shard): NrvProtobuf.Endpoints.Shard = {
    val proto = NrvProtobuf.Endpoints.Shard.newBuilder()

    proto.setToken(shard.token)
    shard.replicas.foreach(r => proto.addReplicas(encodeReplica(r)))

    proto.build()
  }

  def decodeShard(protoShard: NrvProtobuf.Endpoints.Shard): Shard = {

    val protoReplicaSeq = protoShard.getReplicasList.asScala.toSeq
    val replicaSeq = for (replica <- protoReplicaSeq) yield (decodeReplica(replica))
    new Shard(protoShard.getToken, replicaSeq)
  }

  def encodeReplica(replica: Replica): NrvProtobuf.Endpoints.Replica = {
    val proto = NrvProtobuf.Endpoints.Replica.newBuilder()

    proto.setToken(replica.token)
    proto.setSelected(replica.selected)
    proto.setNode(encodeNode(replica.node))

    proto.build()
  }

  def decodeReplica(proto: NrvProtobuf.Endpoints.Replica): Replica = {

    new Replica(proto.getToken, decodeNode(proto.getNode), proto.getSelected)
  }

  def serializeToBytes(entity: AnyRef): Array[Byte] = {
    javaSerialize.encodeAny(entity)
  }

  def serializeFromBytes(bytes: Array[Byte]) = {
    javaSerialize.decodeAny(bytes)
  }
}

