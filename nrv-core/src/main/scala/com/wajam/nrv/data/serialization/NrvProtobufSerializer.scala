package com.wajam.nrv.data.serialization

import scala.collection.JavaConverters._
import com.wajam.nrv.data._
import com.google.protobuf.ByteString
import com.wajam.nrv.cluster.Node
import com.wajam.nrv.service.{Replica, Shard, Endpoints}
import com.wajam.nrv.protocol.codec.{Codec, GenericJavaSerializeCodec}
import java.net.InetAddress

/**
 * Convert NRV principal objects to their Protobuf equivalent back and forth
 *
 */
class NrvProtobufSerializer(val messageCodecs: Map[String, Codec] = Map.empty) {

  val javaSerialize = new GenericJavaSerializeCodec()

  def serializeMessage(message: Message): Array[Byte] = {
    encodeMessage(message).toByteArray
  }

  def deserializeMessage(bytes: Array[Byte]): Message = {
    decodeMessage(NrvProtobuf.Message.parseFrom(bytes))
  }

  private[serialization] def resolveCodec(partialMessage: Message): Codec =  {
    messageCodecs.get(partialMessage.contentType.get).get
  }

  private[serialization] def decodeMValue(protoValue: NrvProtobuf.MValue): MValue = {

    // We can reuse int64 for int32 and bool because of efficient varint in protobuf
    // https://developers.google.com/protocol-buffers/docs/encoding#structure

    import NrvProtobuf.MValue.Type

    protoValue.getType match {
      case Type.INT => MInt(protoValue.getVarintValue().asInstanceOf[Int])
      case Type.LONG => MLong(protoValue.getVarintValue())
      case Type.BOOLEAN => MBoolean(protoValue.getVarintValue() == 1)
      case Type.STRING => MString(protoValue.getStringValue())
      case Type.DOUBLE => MDouble(protoValue.getDoubleValue())
      case Type.LIST => MList(protoValue.getListValueList.asScala.map(decodeMValue(_)))
    }
  }

  private[serialization] def encodeMValue(value: MValue): NrvProtobuf.MValue = {

    import NrvProtobuf.MValue.Type

    val protoValue = NrvProtobuf.MValue.newBuilder()

    value match {
      case value: MInt =>
        protoValue.setVarintValue(value.value)
                  .setType(Type.INT).build()

      case value: MLong =>
        protoValue.setVarintValue(value.value)
                  .setType(Type.LONG).build()

      case value: MBoolean =>
        protoValue.setVarintValue(if (value.value) 1 else 0)
                  .setType(Type.BOOLEAN).build()

      case value: MDouble =>
        protoValue.setDoubleValue(value.value)
                  .setType(Type.DOUBLE).build()

      case value: MString =>
        protoValue.setStringValue(value.value)
                  .setType(Type.STRING).build()

      case value: MList =>
        protoValue.addAllListValue(value.values.map(encodeMValue(_)).asJava)
                  .setType(Type.LIST).build()
    }
  }

  private def encodeMessageMap(map: collection.Map[String, Any],
                               pbFn: (NrvProtobuf.MPair.Builder) => Any) = {

    map.foreach {
      case (key, value) =>
        value match {

          case value: MValue =>
            val protoValue = encodeMValue(value)
            pbFn(NrvProtobuf.MPair.newBuilder().setKey(key).setValue(protoValue))
        }
    }
  }

  private def decodeMessageMap(mList: Iterable[NrvProtobuf.MPair]): Map[String, MValue] = {
    mList.map {
      case (p: NrvProtobuf.MPair) =>
        p.getKey -> decodeMValue(p.getValue())
    }.toMap
  }

  private[serialization] def encodeMessage(message: Message): NrvProtobuf.Message = {

    val protoMessage = NrvProtobuf.Message.newBuilder()

    protoMessage.setCode(message.code)

    protoMessage.setProtocolName(message.protocolName)
    protoMessage.setServiceName(message.serviceName)

    protoMessage.setMethod(message.method)

    protoMessage.setPath(message.path)
    protoMessage.setRendezVousId(message.rendezvousId)

    for (error <- message.error)
      protoMessage.setError(ByteString.copyFrom(javaSerialize.encode(error)))

    protoMessage.setFunction(message.function)

    if (message.source != null)
      protoMessage.setSource(encodeNode(message.source))

    protoMessage.setDestination(encodeEndpoints(message.destination))

    protoMessage.setToken(message.token)

    for (contentType <- message.contentType)
      protoMessage.setContentType(contentType)

    encodeMessageMap(message.parameters, protoMessage.addParameters _)
    encodeMessageMap(message.metadata, protoMessage.addMetadata _)

    val messageDataCodec = resolveCodec(message)

    protoMessage.setMessageData(ByteString.copyFrom(messageDataCodec.encode(message.messageData)))

    protoMessage.build()
  }

  private[serialization] def decodeMessage(protoMessage: NrvProtobuf.Message): Message = {

    val parameters = decodeMessageMap(protoMessage.getParametersList.asScala)

    val metadata = decodeMessageMap(protoMessage.getMetadataList.asScala)

    val destination = decodeEndpoints(protoMessage.getDestination)

    val message = new SerializableMessage(parameters, metadata, null)

    message.code = protoMessage.getCode

    message.protocolName = protoMessage.getProtocolName
    message.serviceName = protoMessage.getServiceName
    message.method = protoMessage.getMethod
    message.path = protoMessage.getPath

    if (protoMessage.hasContentType)
       message.contentType = Some(protoMessage.getContentType)

    // TODO: Modify message.rendezvousId when we won't use JavaSerialization anymore to use a long
    message.rendezvousId = protoMessage.getRendezVousId.asInstanceOf[Int]

    val error = protoMessage.getError

    if (error.size() != 0)
      message.error = Some(javaSerialize.decode(protoMessage.getError.toByteArray).asInstanceOf[Exception])

    message.function = protoMessage.getFunction

    if (protoMessage.hasSource)
      message.source = decodeNode(protoMessage.getSource)

    message.destination = destination
    message.token = protoMessage.getToken

    val messageDataCodec = resolveCodec(message)
    message.messageData = messageDataCodec.decode(protoMessage.getMessageData.toByteArray)

    message
  }

  private[serialization] def encodeNode(node: Node): NrvProtobuf.Node = {
    val protoNode = NrvProtobuf.Node.newBuilder()

    protoNode.setHost(ByteString.copyFrom(node.host.getAddress))

    node.ports.foreach {
      case (key, value) =>
        protoNode.addPorts(NrvProtobuf.Int32Pair.newBuilder().setKey(key).setValue(value))
    }

    protoNode.build()
  }

  private[serialization] def decodeNode(protoNode: NrvProtobuf.Node): Node = {
    val portList = protoNode.getPortsList.asScala.toSeq
    val portMap = for (p <- portList) yield ((p.getKey, p.getValue))
    new Node(InetAddress.getByAddress(protoNode.getHost.toByteArray), portMap.toMap)
  }

  private[serialization] def encodeEndpoints(endpoints: Endpoints): NrvProtobuf.Endpoints = {
    val protoEndpoint = NrvProtobuf.Endpoints.newBuilder()

    endpoints.shards.foreach {
      case (shard) =>
        protoEndpoint.addShards(encodeShard(shard))
    }

    protoEndpoint.build()
  }

  private[serialization] def decodeEndpoints(protoEndpoints: NrvProtobuf.Endpoints): Endpoints = {
    val protoShardSeq = protoEndpoints.getShardsList.asScala.toSeq
    val shardSeq = for (shard <- protoShardSeq) yield (decodeShard(shard))
    new Endpoints(shardSeq)
  }

  private[serialization] def encodeShard(shard: Shard): NrvProtobuf.Endpoints.Shard = {
    val proto = NrvProtobuf.Endpoints.Shard.newBuilder()

    proto.setToken(shard.token)
    shard.replicas.foreach(r => proto.addReplicas(encodeReplica(r)))

    proto.build()
  }

  private[serialization] def decodeShard(protoShard: NrvProtobuf.Endpoints.Shard): Shard = {

    val protoReplicaSeq = protoShard.getReplicasList.asScala.toSeq
    val replicaSeq = for (replica <- protoReplicaSeq) yield (decodeReplica(replica))
    new Shard(protoShard.getToken, replicaSeq)
  }

  private[serialization] def encodeReplica(replica: Replica): NrvProtobuf.Endpoints.Replica = {
    val proto = NrvProtobuf.Endpoints.Replica.newBuilder()

    proto.setToken(replica.token)
    proto.setSelected(replica.selected)
    proto.setNode(encodeNode(replica.node))

    proto.build()
  }

  private[serialization] def decodeReplica(proto: NrvProtobuf.Endpoints.Replica): Replica = {

    new Replica(proto.getToken, decodeNode(proto.getNode), proto.getSelected)
  }
}

