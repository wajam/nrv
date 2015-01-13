package com.wajam.nrv.extension.json

import scala.concurrent.{ExecutionContext, Future}

import net.liftweb.json.{Formats, _}
import org.scalatest.FlatSpec
import org.scalatest.Matchers._

import com.wajam.nrv.cluster.{Cluster, LocalNode, StaticClusterManager}
import com.wajam.nrv.extension.http.{NotFoundException, ServiceUnavailableException}
import com.wajam.nrv.extension.integration.StringHttpClientOperations
import com.wajam.nrv.extension.json.JsonApiDSLSpec._
import com.wajam.nrv.extension.json.codec.JsonCodec
import com.wajam.nrv.extension.json.integration.JsonHttpClientOperations
import com.wajam.nrv.protocol.HttpProtocol
import com.wajam.nrv.service.Service

class JsonApiDSLSpec extends FlatSpec {

  trait TestableJsonApi extends JsonApiDSL {
    def ec: ExecutionContext = ExecutionContext.global
  }

  trait JsonTestClient extends JsonHttpClientOperations {

    implicit def formats: Formats

    def getObject[T](path: String)(implicit mf: scala.reflect.Manifest[T]): Option[T] = {
      get(path) match {
        case (_, JNothing) => None
        case (_, value) => Some(value.extract[T])
      }
    }

    def postObject[T](path: String, data: Option[T])(implicit mf: scala.reflect.Manifest[T]): Option[T] = {
      val obj = data.map(Extraction.decompose).getOrElse(JNothing)
      post(path, obj) match {
        case (_, JNothing) => None
        case (_, value) => Some(value.extract[T])
      }
    }

    def putObject[T](path: String, data: Option[T])(implicit mf: scala.reflect.Manifest[T]): Option[T] = {
      val obj = data.map(Extraction.decompose).getOrElse(JNothing)
      put(path, obj) match {
        case (_, JNothing) => None
        case (_, value) => Some(value.extract[T])
      }
    }

    def deleteObject[T](path: String)(implicit mf: scala.reflect.Manifest[T]): Option[T] = {
      delete(path) match {
        case (_, JNothing) => None
        case (_, value) => Some(value.extract[T])
      }
    }
  }

  trait TestClient {
    def json: JsonTestClient
    def text: StringHttpClientOperations
  }

  trait Setup {

    def testWith(service: Service with TestableJsonApi)(test: (TestClient) => Unit): Unit = {

      val httpPort = 8778
      val localNode = new LocalNode("0.0.0.0", Map("nrv" -> 6753, "http" -> httpPort))
      val clusterManager = new StaticClusterManager().addMembers(service, List("0:127.0.0.1:nrv=6753"))
      val cluster = new Cluster(localNode, clusterManager)
      val httpProtocol = new HttpProtocol("http", localNode, 1000, 1).registerCodec("application/json", new JsonCodec)
      cluster.registerProtocol(httpProtocol)
      service.applySupport(supportedProtocols = Some(Set(httpProtocol)))
      cluster.registerService(service)

      val client = new TestClient {
        lazy val json = new JsonTestClient {
          protected def port: Int = httpPort
          def formats: Formats = service.formats
        }

        lazy val text = new StringHttpClientOperations {
          protected def port: Int = httpPort
        }
      }

      try {
        cluster.start()
        test(client)
      } finally {
        cluster.stop()
      }
    }

  }

  "JsonApi GET" should "returns expected resources" in new Setup {

    val expectedResources = List(Resource("1"), Resource("2"), Resource("3"))

    val service = new Service("test") with TestableJsonApi {
      GET("/resources") -> { (request, ec) =>
        Future.successful(Some(expectedResources))
      }

      GET("/resources/:id") -> { (request, ec) =>
        Future.successful(Some(Resource(request.paramString("id"))))
      }
    }

    testWith(service) { client =>
      client.json.getObject[List[Resource]]("/resources") should be(Some(expectedResources))
      client.json.getObject[Resource]("/resources/1") should be(Some(Resource("1")))
    }
  }

  it should "handle unexpected and expected server errors" in new Setup {

    val service = new Service("test") with TestableJsonApi {
      GET("/unexpected_error") -> { (request, ec) =>
        throw new Exception()
      }
      GET("/not_found_error") -> { (request, ec) =>
        throw new NotFoundException()
      }

      GET("/unavailable_error") -> { (request, ec) =>
        Future.failed(new ServiceUnavailableException())
      }
    }

    testWith(service) { client =>
      client.json.get("/unexpected_error")._1 should be (500)
      client.json.get("/not_found_error")._1 should be (404)
      client.json.get("/unavailable_error")._1 should be (503)
    }

  }

  it should "returns nothing when server send an empty response" in new Setup {

    val service = new Service("test") with TestableJsonApi {
      GET("/empty") -> { (request, ec) =>
        Future.successful(None)
      }
    }

    testWith(service) { client =>
      client.json.get("/empty") should be ((200, JNothing))
      client.json.getObject("/empty") should be (None)
    }

  }

  "JsonApi POST" should "receive and returns expected resource" in new Setup {

    val expectedResources = Resource("1")

    val service = new Service("test") with TestableJsonApi {
      POST("/resources") ->> { (resource: Resource, request, ec) =>
        Future.successful(Some(resource))
      }

      POST("/resources/optional") -?> { (resource: Option[Resource], request, ec) =>
        Future.successful(resource)
      }
    }

    testWith(service) { client =>

      val expectedResource = Resource("1")

      client.json.postObject("/resources", Some(expectedResource)) should be(Some(expectedResources))
      client.json.postObject("/resources/optional", Some(expectedResource)) should be(Some(expectedResources))
      client.json.postObject[Resource]("/resources/optional", None) should be(None)
      client.json.post("/resources/optional", "") should be((200, JNothing))
      client.text.post("/resources/optional", "") should be((200, ""))
      client.text.post("/resources/optional", "not json") should be((400, ""))
    }
  }

  "JsonApi PUT" should "receive and returns expected resource" in new Setup {

    val expectedResources = Resource("1")

    val service = new Service("test") with TestableJsonApi {
      PUT("/resources") ->> { (resource: Resource, request, ec) =>
        Future.successful(Some(resource))
      }

      PUT("/resources/optional") -?> { (resource: Option[Resource], request, ec) =>
        Future.successful(resource)
      }
    }

    testWith(service) { client =>

      val expectedResource = Resource("1")

      client.json.putObject("/resources", Some(expectedResource)) should be(Some(expectedResources))
      client.json.putObject("/resources/optional", Some(expectedResource)) should be(Some(expectedResources))
      client.json.putObject[Resource]("/resources/optional", None) should be(None)
      client.json.put("/resources/optional", "") should be((200, JNothing))
      client.text.put("/resources/optional", "") should be((200, ""))
      client.text.put("/resources/optional", "not json") should be((400, ""))
    }
  }

  "JsonApi DELETE" should "delete expected resource" in new Setup {

    val expectedResources = Resource("1")

    val service = new Service("test") with TestableJsonApi {
      DELETE("/resources/:id") -> { (request, ec) =>
        Future.successful(Some(Resource(request.paramString("id"))))
      }
    }

    testWith(service) { client =>
      client.json.deleteObject[Resource]("/resources/1") should be(Some(Resource("1")))
      client.json.deleteObject[Resource]("/resources/2") should be(Some(Resource("2")))
    }
  }

}

object JsonApiDSLSpec {
  case class Resource(value: String)
}
