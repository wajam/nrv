package com.wajam.nrv.protocol

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.scalatest.matchers.ShouldMatchers._
import org.jboss.netty.handler.codec.http._
import com.wajam.nrv.cluster.{LocalNode, StaticClusterManager, Cluster}
import com.wajam.nrv.service._
import com.wajam.nrv.data._
import com.wajam.nrv.protocol.HttpProtocol._
import java.io._
import java.nio.ByteBuffer
import scala.Some
import scala.Some
import scala.Some

@RunWith(classOf[JUnitRunner])
class TestNrvProtocol extends FunSuite with BeforeAndAfter {

  var localNode: LocalNode = null

  var cluster: Cluster = null
  var action: Action = null
  var service: Service = null
  var nrvProtocol: NrvProtocol = null

  var tempFile: File = null
  val fileSize: Int = 1024

  before {
    localNode = new LocalNode("localhost", Map("nrv" -> 19191, "test" -> 1909))

    cluster = new Cluster(localNode, new StaticClusterManager)

    nrvProtocol = new NrvProtocol(localNode, 1000, 100)

    action = new Action("action", (msg) => {}, ActionMethod.ANY)

    service = new Service("service")

    service.registerAction(action)
    service.applySupport(protocol = Some(nrvProtocol))

    cluster.registerProtocol(nrvProtocol)
    cluster.registerService(service)

    cluster.start()
  }

  after {
    tempFile.delete()
    cluster.stop()
  }

  test("should load an InputStream into memory and generate a NRV message") {

    /* Create a temporary file and fill it with 1MB of data */
    tempFile = File.createTempFile("file", ".tmp")

    val os = new FileOutputStream(tempFile)

    var size = 0
    while(size < fileSize) {
      val content = "a".getBytes()
      os.write(content)
      os.flush()
      size += content.length
    }
    os.close()

    val msg = new OutMessage(
      data = new FileInputStream(tempFile),
      meta = Map("Content-Type" -> "text/plain")
    )

    msg.destination = new Endpoints(Seq(new Shard(0, Seq(new Replica(0, cluster.localNode)))))

    // Simulate switchboard configuration
    msg.source = cluster.localNode
    msg.serviceName = service.name
    msg.path = action.path.buildPath(msg.parameters)

    val generated = nrvProtocol.generate(msg)
    val parsed = nrvProtocol.parse(generated)

    val is = parsed.messageData.asInstanceOf[Array[Byte]]

    val fis = new FileInputStream(tempFile)

    /* Check that the content is correct */
    for(i <- 0 to fileSize - 1)
      assert(is(i) === fis.read())

    /* Check that the size is correct */
    assert(fis.read() == -1)
  }
}
