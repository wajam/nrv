package com.wajam.nrv.data.serialization

import com.wajam.nrv.data.Message
import java.nio.ByteBuffer
import com.google.protobuf.ByteString
import com.wajam.nrv.cluster.Node
import com.wajam.nrv.service.{Shard, Endpoints}


class NRVProtosCodec {

  def encode(message: Message) : NRVProtos.Message = {

    val protoMessage = NRVProtos.Message.newBuilder()

    protoMessage.setProtocolName(message.protocolName)
    protoMessage.setServiceName(message.serviceName)

    protoMessage.setMethod(message.method.asInstanceOf[Int])

    protoMessage.setPath(message.path)
    protoMessage.setRendezVousId(message.rendezvousId)

    protoMessage.setError(ByteString.copyFrom(serializeToBytes(message.error)))

    protoMessage.setFunction(message.function)

    protoMessage.setSource(encodeNode(message.source))

    protoMessage.setDestination(encodeEndpoints(message.destination))

    protoMessage.setToken(message.token)

    message.parameters.foreach{
      case (key, value) =>
        protoMessage.addParameters(NRVProtos.StringPair.newBuilder().setKey(key).setValue(value.toString()))}

    message.metadata.foreach{
      case (key, value) =>
        protoMessage.addMetadata(NRVProtos.StringPair.newBuilder().setKey(key).setValue(value.toString()))}

    protoMessage.setMessageData(message.messageData)

    protoMessage.build()
  }

  def decode(data: ByteBuffer) = {

  }

  def encodeNode(node: Node) = {

  }

  def decodeNode(node: NRVProtos.Node) = {

  }

  def encodeEndpoints(endpoint: Endpoints) = {

  }

  def decodeEndpoints(node: NRVProtos.Endpoints) = {

  }

  def encodeShard(shard: Shard) = {

  }

  def decodeShard(node: NRVProtos.Endpoint.Shard) = {

  }

  private def serializeToBytes(entity: Any) : ByteBuffer {

  }

  private def serializeFromBytes(bytes: ByteBuffer) {

  }
}

