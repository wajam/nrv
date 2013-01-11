package com.wajam.nrv.data.serialization

import com.wajam.nrv.data.Message
import com.google.protobuf.ByteString
import com.wajam.nrv.cluster.Node
import com.wajam.nrv.service.{Shard, Endpoints}
import com.wajam.nrv.protocol.codec.{Codec, GenericJavaSerializeCodec}

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

  def decodeNode(node: NrvProtos.Node): Node = {
    sys.error("unimplemented")
  }

  def encodeEndpoints(endpoints: Endpoints): NrvProtos.Endpoints = {
    val protoEndpoint = NrvProtos.Endpoints.newBuilder()

    endpoints.shards.foreach{
      case (shard) =>
        protoEndpoint.addShards(encodeShard(shard))}

    protoEndpoint.build()
  }

  def decodeEndpoints(node: NrvProtos.Endpoints) = {
    sys.error("unimplemented")
  }

  def encodeShard(shard: Shard): NrvProtos.Endpoints.Shard = {
    sys.error("unimplemented")
  }

  def decodeShard(node: NrvProtos.Endpoints.Shard) = {
    sys.error("unimplemented")
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

