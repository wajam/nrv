package com.wajam.nrv.integration

import java.io._
import java.net.{HttpURLConnection, URL}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, BeforeAndAfter}
import org.scalatest.matchers.ShouldMatchers
import com.wajam.nrv.data._
import com.wajam.nrv.service._
import com.wajam.nrv.cluster.{LocalNode, StaticClusterManager, Cluster}
import com.wajam.nrv.protocol.HttpProtocol

@RunWith(classOf[JUnitRunner])
class TestHttpNettyTransport extends FunSuite with BeforeAndAfter with ShouldMatchers {

  var localNode: LocalNode = null

  var cluster: Cluster = null
  var action: Action = null
  var service: Service = null
  var httpProtocol: HttpProtocol = null

  val notifier = new Object()
  var received: AnyRef = null

  var tempFile: File = null

  before {
    localNode = new LocalNode("127.0.0.1", Map("nrv" -> 12345, "http" -> 8000))

    cluster = new Cluster(localNode, new StaticClusterManager)

    service = new Service("service")

    httpProtocol = new HttpProtocol("http", localNode, 1000, 100)

    /* Create a temporary file and fill it with 1MB of data */
    tempFile = File.createTempFile("file", ".tmp")

    val os = new FileOutputStream(tempFile)

    var size = 0
    while(size < 1024*1024) {
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
    /* Check that the first bytes are correct */
    for(i <- 0 to 10)
      assert(connection.getContent.asInstanceOf[InputStream].read() === new FileInputStream(tempFile).read())
  }
}
