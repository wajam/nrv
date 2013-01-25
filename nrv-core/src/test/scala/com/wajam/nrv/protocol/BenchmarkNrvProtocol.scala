package com.wajam.nrv.protocol

import com.wajam.nrv.data.{MessageType, SerializableMessage}
import com.wajam.nrv.service.{Endpoints, Shard, Replica, ActionMethod}
import java.net.InetAddress
import com.wajam.nrv.cluster.{LocalNode, Node}
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import com.wajam.nrv.data._
import scala.Array

// TODO: Remove after initial test
@RunWith(classOf[JUnitRunner])
class BenchmarkNrvProtocol extends FunSuite with BeforeAndAfter  {

  def generateException() = {

    try {
      val x, y = 0
      x / y
      null
    }
    catch {
      case ex:Exception => ex
    }
  }

  private def makeMessage() = {
    val message = new SerializableMessage()

    message.protocolName = "Nrv"
    message.serviceName = "Nrv"
    message.method = ActionMethod.PUT
    message.path = "Yellow brick road"
    message.rendezvousId = 1024

    message.error = Some(generateException())

    message.function = MessageType.FUNCTION_CALL

    val localhost = InetAddress.getLocalHost

    message.source = new Node(localhost, Map(("nrv", 1024)))

    val replica1 = new Replica(1024, new Node(localhost, Map(("nrv", 1025))))

    val shard1 = new Shard(1024, Seq(replica1))

    message.destination = new Endpoints(Seq(shard1))

    message.token = 1024

    message.parameters += "Key" -> "Value"
    message.metadata += "CONTENT-TYPE" -> "text/plain"
    message.messageData = "Blob Of Data"

    message.metadata += "Key1" -> 1
    message.parameters += "Key1" -> 1

    message.metadata += "Key2" -> 1.0
    message.parameters += "Key2" -> 1.0

    message.metadata += "Key3" -> new Pair("A", 1)
    message.parameters += "Key3" -> new Pair("A", 1)

    // Here you go a message with possible value set (attachments is not serialized)

    message
  }

  private def testEncodeDecode(message: Message) = {
    val repetitions: Int = 1000

    val nodeA: LocalNode = new LocalNode("127.0.0.1", Map("nrv" -> 12345))
    val nodeB: LocalNode = new LocalNode("127.0.0.1", Map("nrv" -> 12346))

    val protocolV2 = new NrvProtocol(nodeA, protocolVersion = NrvProtocolVersion.V2)
    val protocolV1 = new NrvProtocol(nodeB, protocolVersion = NrvProtocolVersion.V1)


    for (test <- Map(("V1", protocolV1), ("V2",protocolV2))) {
      for (i <- 1 to 5)
      {
        val testBytes = test._2.generate(message)

        val startTimeNanos  = System.nanoTime()

        for (r <- 1 to repetitions) {
          val bytes = test._2.generate(message)
        }

        val totalTimeRead = System.nanoTime()

        for (r <- 1 to repetitions) {
          val message = test._2.parse(testBytes)
        }

        val totalTimeWrite = System.nanoTime()
        val total = System.nanoTime()

        val timeWrite = (totalTimeRead - startTimeNanos) / repetitions
        val timeRead = (totalTimeWrite - totalTimeRead) / repetitions
        val timeNanos = (total - startTimeNanos) / repetitions
        val text = "%d %s\twrite=%,dns read=%,dns total=%,dns\n".format(i, test._1, timeWrite, timeRead, timeNanos)
        System.out.print(text)

        System.gc()
        Thread.sleep(1000)
      }
    }
  }

  test("v2 is faster than v1, empty message") {
    val message = new SerializableMessage()

    testEncodeDecode(message)
  }

  test("v2 is faster than v1, full message") {
    val message = makeMessage()

    testEncodeDecode(message)
  }

}
