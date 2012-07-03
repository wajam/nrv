package com.wajam.nrv.service

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import com.wajam.nrv.cluster.Node
import com.wajam.nrv.service.ActionPath._
import org.scalatest.FunSuite
import com.wajam.nrv.data.{OutMessage, InMessage}

@RunWith(classOf[JUnitRunner])
class TestResolver extends FunSuite {
  val service = new Service("test")
  service.addMember(5, new Node("localhost", Map("nrv" -> 12345)))
  service.addMember(7, new Node("localhost", Map("nrv" -> 12346)))
  service.addMember(9, new Node("localhost", Map("nrv" -> 12346)))
  service.addMember(12, new Node("localhost", Map("nrv" -> 12346)))
  service.addMember(20, new Node("localhost", Map("nrv" -> 12346)))
  service.addMember(30, new Node("localhost", Map("nrv" -> 12346)))

  test("fullpath extractor should hash full path") {
    assert(Resolver.TOKEN_FULLPATH("/test/:par", "/test/parval") == Resolver.hashData("/test/parval"))
  }

  test("param hash path extractor should hash param path") {
    assert(Resolver.TOKEN_HASH_PARAM("par")("/test/:par", "/test/parval") == Resolver.hashData("parval"))
  }

  test("param path extractor should hash param path") {
    assert(Resolver.TOKEN_PARAM("token")("/test/:token", "/test/5345435") == 5345435)
  }

  test("resolver with count should return count") {
    val resolver = new Resolver(replica = 3)
    val endsPoints = resolver.resolve(service, 19)
    assert(endsPoints.size == 3)
    assert(endsPoints(0).token == 20, endsPoints(0).token)
    assert(endsPoints(1).token == 30, endsPoints(1).token)
    assert(endsPoints(2).token == 5, endsPoints(2).token)
  }

  test("resolver with a sorter should use that sorter") {
    val resolver = new Resolver(replica = 3, sorter = (m1, m2) => {
      m1.token < m2.token
    })
    val endsPoints = resolver.resolve(service, 19)
    assert(endsPoints.size == 3)
    assert(endsPoints(0).token == 5, endsPoints(0).token)
    assert(endsPoints(1).token == 20, endsPoints(1).token)
    assert(endsPoints(2).token == 30, endsPoints(2).token)
  }

  test("resolver with constraint should remove constrainted") {
    val resolver = new Resolver(replica = 3, constraints = (cur, m) => {
      m.token != 20
    })
    val endsPoints = resolver.resolve(service, 19)
    assert(endsPoints.size == 3)
    assert(endsPoints(0).token == 30, endsPoints(0).token)
    assert(endsPoints(1).token == 5, endsPoints(1).token)
    assert(endsPoints(2).token == 7, endsPoints(2).token)
  }

  test("resolver add token to message") {
    val tokenValue = 42
    val action = new Action("test", (message: InMessage) => {})
    action.supportedBy(service)
    val resolver = new Resolver(tokenExtractor = (actionPath: ActionPath, path: String) => tokenValue)

    val outMessage = new OutMessage()
    assert(outMessage.token != tokenValue)
    resolver.handleOutgoing(action, outMessage)
    assert(tokenValue === outMessage.token)

    val inMessage = new InMessage()
    assert(inMessage != tokenValue)
    resolver.handleIncoming(action, inMessage)
    assert(tokenValue === inMessage.token)
  }
}
