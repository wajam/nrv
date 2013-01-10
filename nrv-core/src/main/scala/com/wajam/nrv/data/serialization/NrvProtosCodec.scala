package com.wajam.nrv.data.serialization

import com.wajam.nrv.data.Message
import com.google.protobuf.ByteString
import com.wajam.nrv.cluster.Node
import com.wajam.nrv.service.{Shard, Endpoints}
import com.wajam.nrv.protocol.codec.JavaSerializeCodec

/**
 * Convert NRV principal objects to their Protobuf equivalent back and forth
 */
class NrvProtosCodec {

  val javaSerialize = new JavaSerializeCodec

  def encodeMessage(message: Message) : NRVProtos.Message = {

    val protoMessage = NRVProtos.Message.newBuilder()

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
        protoMessage.addParameters(NRVProtos.StringPair.newBuilder().setKey(key).setValue(value.toString))}

    message.metadata.foreach{
      case (key, value) =>
        protoMessage.addMetadata(NRVProtos.StringPair.newBuilder().setKey(key).setValue(value.toString))}

    protoMessage.setMessageData(ByteString.copyFrom(encodeUsingCodec(message.messageData)))

    protoMessage.build()
  }

  def decodeMessage(data: Array[Byte]) : Message = {
    sys.error("unimplemented")
  }

  def encodeNode(node: Node): NRVProtos.Node = {
    sys.error("unimplemented")
  }

  def decodeNode(node: NRVProtos.Node): Node = {
    sys.error("unimplemented")
  }

  def encodeEndpoints(endpoint: Endpoints) : NRVProtos.Endpoints = {
    sys.error("unimplemented")
  }

  def decodeEndpoints(node: NRVProtos.Endpoints) = {
    sys.error("unimplemented")
  }

  def encodeShard(shard: Shard) = {
    sys.error("unimplemented")
  }

  def decodeShard(node: NRVProtos.Endpoints.Shard) = {
    sys.error("unimplemented")
  }

  def encodeUsingCodec(messageData: Any): Array[Byte] = {
    sys.error("unimplemented")
  }

  def decodeUsingCodec(messageRawData: Array[Byte]): Any = {
    sys.error("unimplemented")
  }

  def serializeToBytes(entity: AnyRef) : Array[Byte] = {
    javaSerialize.encodeAny(entity)
  }

  def serializeFromBytes(bytes: Array[Byte]) = {
    javaSerialize.decodeAny(bytes)
  }
}

