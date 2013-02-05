package com.wajam.nrv.data.serialization

import scala.collection.JavaConverters._
import com.wajam.nrv.data.{Message, SerializableMessage}
import com.google.protobuf.ByteString
import com.wajam.nrv.cluster.Node
import com.wajam.nrv.service.{Replica, Shard, Endpoints}
import com.wajam.nrv.protocol.codec.{Codec, GenericJavaSerializeCodec}
import java.net.InetAddress
import com.wajam.nrv.data.serialization.NrvProtobuf.{AnyPair, StringListPair}

/**
 * Convert NRV principal objects to their Protobuf equivalent back and forth
 *
 * For metadata, parameters migration DEBUG:
 *
 * dropString: Enable the drop of the new string to avoid the new string format to be picked up in client
 * (disable the new format)
 *
 * dropAny: Enable the drop of the old any. Thus only to the new string format will be available, very usefull to find
 * case not migrated.
 */
class NrvProtobufSerializer(val dropString : Boolean = false, val dropAny : Boolean = true) {

  val javaSerialize = new GenericJavaSerializeCodec()

  def serializeMessage(message: Message, messageDataCodec: Codec = javaSerialize): Array[Byte] = {
    encodeMessage(message, messageDataCodec).toByteArray
  }

  def deserializeMessage(bytes: Array[Byte], messageDataCodec: Codec = javaSerialize): Message = {
    decodeMessage(NrvProtobuf.Message.parseFrom(bytes), messageDataCodec)
  }

  private[serialization] def encodeMessage(message: Message, messageDataCodec: Codec): NrvProtobuf.Message = {

    val protoMessage = NrvProtobuf.Message.newBuilder()

    protoMessage.setCode(message.code)

    protoMessage.setProtocolName(message.protocolName)
    protoMessage.setServiceName(message.serviceName)

    protoMessage.setMethod(message.method)

    protoMessage.setPath(message.path)
    protoMessage.setRendezVousId(message.rendezvousId)

    for (error <- message.error)
      protoMessage.setError(ByteString.copyFrom(serializeToBytes(error)))

    protoMessage.setFunction(message.function)

    if (message.source != null)
      protoMessage.setSource(encodeNode(message.source))

    protoMessage.setDestination(encodeEndpoints(message.destination))

    protoMessage.setToken(message.token)

    if (!dropAny ) {
      message.parameters.foreach {
        case (key, value) =>
          protoMessage.addParametersAny(NrvProtobuf.AnyPair.newBuilder().setKey(key).setValue(ByteString.copyFrom(serializeToBytes(value.asInstanceOf[AnyRef]))))
      }

      message.metadata.foreach {
        case (key, value) =>
          protoMessage.addMetadataAny(NrvProtobuf.AnyPair.newBuilder().setKey(key).setValue(ByteString.copyFrom(serializeToBytes(value.asInstanceOf[AnyRef]))))
      }
    }

    if (!dropString) {
      message.parametersNew.foreach {
        case (key, value) =>
          protoMessage.addParameters(NrvProtobuf.StringListPair.newBuilder().setKey(key).addValue(value.asInstanceOf[String]))
      }

      message.metadataNew.foreach {
        case (key, value) =>
          protoMessage.addMetadata(NrvProtobuf.StringListPair.newBuilder().setKey(key).addValue(value.asInstanceOf[String]))

      }
    }

    protoMessage.setMessageData(ByteString.copyFrom(messageDataCodec.encode(message.messageData)))

    protoMessage.build()
  }

  private[serialization] def decodeMessage(protoMessage: NrvProtobuf.Message, messageDataCodec: Codec): Message = {

    val parameters = new collection.mutable.HashMap[String, Any]
    val metadata = new collection.mutable.HashMap[String, Any]
    val parametersNew = new collection.mutable.HashMap[String, Seq[String]]
    val metadataNew = new collection.mutable.HashMap[String, Seq[String]]

    protoMessage.getParametersAnyList.asScala.foreach {
      case (p: AnyPair) =>
        parameters += p.getKey -> serializeFromBytes(p.getValue.toByteArray)
    }

    protoMessage.getParametersList.asScala.foreach {
      case (p: StringListPair) =>
        parametersNew += p.getKey -> p.getValueList.asScala.toSeq
    }

    protoMessage.getMetadataAnyList.asScala.foreach {
      case (p: AnyPair) =>
        metadata += p.getKey -> serializeFromBytes(p.getValue.toByteArray)
    }

    protoMessage.getMetadataList.asScala.foreach {
      case (p: StringListPair) =>
        metadataNew += p.getKey -> p.getValueList.asScala.toSeq
    }

    val messageData = messageDataCodec.decode(protoMessage.getMessageData.toByteArray)

    val destination = decodeEndpoints(protoMessage.getDestination)

    val message = new SerializableMessage(parameters, metadata, messageData)

    message.parametersNew ++= parametersNew
    message.metadataNew ++= metadataNew

    message.code = protoMessage.getCode

    message.protocolName = protoMessage.getProtocolName
    message.serviceName = protoMessage.getServiceName
    message.method = protoMessage.getMethod
    message.path = protoMessage.getPath

    // TODO: Modify message.rendezvousId when we won't use JavaSerialization anymore to use a long
    message.rendezvousId = protoMessage.getRendezVousId.asInstanceOf[Int]

    val error = protoMessage.getError

    if (error.size() != 0)
      message.error = Some(serializeFromBytes(protoMessage.getError.toByteArray).asInstanceOf[Exception])

    message.function = protoMessage.getFunction

    if (protoMessage.hasSource)
      message.source = decodeNode(protoMessage.getSource)

    message.destination = destination
    message.token = protoMessage.getToken

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

  private[serialization] def serializeToBytes(entity: AnyRef): Array[Byte] = {
    javaSerialize.encodeAny(entity)
  }

  private[serialization] def serializeFromBytes(bytes: Array[Byte]) = {
    javaSerialize.decodeAny(bytes)
  }
}

