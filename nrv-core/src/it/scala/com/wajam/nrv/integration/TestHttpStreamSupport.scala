package com.wajam.nrv.integration

import java.io._
import java.net.{HttpURLConnection, URL}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, BeforeAndAfter}
import org.scalatest.{Matchers => ShouldMatchers}
import com.wajam.nrv.data._
import com.wajam.nrv.service._
import com.wajam.nrv.cluster.{LocalNode, StaticClusterManager, Cluster}
import com.wajam.nrv.protocol.HttpProtocol

@RunWith(classOf[JUnitRunner])
class TestHttpStreamSupport extends FunSuite with BeforeAndAfter with ShouldMatchers {

  var localNode: LocalNode = null

  var cluster: Cluster = null
  var action: Action = null
  var service: Service = null
  var httpProtocol: HttpProtocol = null

  var received: AnyRef = null

  var tempFile: File = null
  val fileSize: Int = 1024

  before {
    localNode = new LocalNode("127.0.0.1", Map("nrv" -> 12345, "http" -> 8000))

    cluster = new Cluster(localNode, new StaticClusterManager)

    service = new Service("service")

    httpProtocol = new HttpProtocol("http", localNode, 1000, 100)

    /* Create a temporary file and fill it with data */
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

    action = new Action("/big/file", (msg) => {
        msg.reply(new OutMessage(
          data = new FileInputStream(tempFile),
          meta = Map("Content-Type" -> "text/plain")
        ))
      }, ActionMethod.GET)

    service.registerAction(action)
    service.applySupport(protocol = Some(httpProtocol))

    cluster.registerProtocol(httpProtocol)
    cluster.registerService(service)

    cluster.start()
  }

  after {
    tempFile.delete()
    cluster.stop()
  }

  test("should stream a big file in a chunked HTTP response") {
    val url = new URL("http://" + localNode.hostname + ":" + localNode.ports("http") + action.path.toString)

    val connection = url.openConnection().asInstanceOf[HttpURLConnection]
    connection.setDoOutput(true)
    connection.setRequestMethod("GET")
    connection.setRequestProperty("Content-Type", "text/plain; charset=utf-8")

    assert(connection.getResponseCode === HttpURLConnection.HTTP_OK)
    assert(connection.getHeaderField("Transfer-Encoding") === "chunked")

    /* Check that the content is correct */
    val fis = new FileInputStream(tempFile)

    /* Check until the {fileSize + 1}th byte, which should return -1 in both cases */
    for(i <- 0 to fileSize)
      assert(connection.getContent.asInstanceOf[InputStream].read() === fis.read())
  }
}
