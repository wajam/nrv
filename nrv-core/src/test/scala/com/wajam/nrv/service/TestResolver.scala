package com.wajam.nrv.service

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import com.wajam.nrv.cluster.Node
import com.wajam.nrv.service.ActionPath._
import org.scalatest.{BeforeAndAfter, FunSuite}
import com.wajam.nrv.data.{OutMessage, InMessage}

@RunWith(classOf[JUnitRunner])
class TestResolver extends FunSuite with BeforeAndAfter {
  var service: Service = null
  var memb5: ServiceMember = null
  var memb7: ServiceMember = null
  var memb9: ServiceMember = null
  var memb12: ServiceMember = null
  var memb20: ServiceMember = null
  var memb30: ServiceMember = null

  before {
    service = new Service("test")
    memb5 = service.addMember(new ServiceMember(5, new Node("localhost", Map("nrv" -> 12345))))
    memb7 = service.addMember(new ServiceMember(7, new Node("localhost", Map("nrv" -> 12346))))
    memb9 = service.addMember(new ServiceMember(9, new Node("localhost", Map("nrv" -> 12346))))
    memb12 = service.addMember(new ServiceMember(12, new Node("localhost", Map("nrv" -> 12346))))
    memb20 = service.addMember(new ServiceMember(20, new Node("localhost", Map("nrv" -> 12346))))
    memb30 = service.addMember(new ServiceMember(30, new Node("localhost", Map("nrv" -> 12346))))

    for (member <- service.members) {
      member.setStatus(MemberStatus.Up, triggerEvent = false)
    }
  }

  test("fullpath extractor should hash full path") {
    assert(Resolver.TOKEN_FULLPATH("/test/:par", "/test/parval") == Resolver.hashData("/test/parval"))
  }

  test("param hash path extractor should hash param path") {
    assert(Resolver.TOKEN_HASH_PARAM("par")("/test/:par", "/test/parval") == Resolver.hashData("parval"))
  }

  test("param path extractor should hash param path") {
    assert(Resolver.TOKEN_PARAM("token")("/test/:token", "/test/5345435") == 5345435)
  }

  test("resolver returns replicas matching asked replica count") {
    val resolver = new Resolver(replica = 3)
    val endsPoints = resolver.resolve(service, 19).selectedReplicas
    assert(endsPoints.size == 3)
    assert(endsPoints(0).token == 20, endsPoints(0).token)
    assert(endsPoints(1).token == 30, endsPoints(1).token)
    assert(endsPoints(2).token == 5, endsPoints(2).token)
  }

  test("resolver returns replicas even if they are not UP, but mark DOWNs as disabled") {
    memb30.setStatus(MemberStatus.Down, triggerEvent = false)

    val resolver = new Resolver(replica = 3)
    val endsPoints = resolver.resolve(service, 19).shards(0).replicas
    assert(endsPoints.size == 3)
    assert(endsPoints(0).token == 20, endsPoints(0).token)
    assert(endsPoints(0).selected)
    assert(endsPoints(1).token == 30, endsPoints(1).token)
    assert(!endsPoints(1).selected)
    assert(endsPoints(2).token == 5, endsPoints(2).token)
    assert(endsPoints(2).selected)
    assert(resolver.resolve(service, 19).selectedReplicas.size == 2)


    memb20.setStatus(MemberStatus.Down, triggerEvent = false)
    memb30.setStatus(MemberStatus.Down, triggerEvent = false)
    memb5.setStatus(MemberStatus.Down, triggerEvent = false)
    assert(resolver.resolve(service, 19).selectedReplicas.size == 0)
  }

  test("resolver with a sorter should use that sorter") {
    val resolver = new Resolver(replica = 3, sorter = (m1, m2) => {
      m1.token < m2.token
    })
    val endsPoints = resolver.resolve(service, 19).selectedReplicas
    assert(endsPoints.size == 3)
    assert(endsPoints(0).token == 5, endsPoints(0).token)
    assert(endsPoints(1).token == 20, endsPoints(1).token)
    assert(endsPoints(2).token == 30, endsPoints(2).token)
  }

  test("resolver with constraint should remove constrainted") {
    val resolver = new Resolver(replica = 3, constraints = (cur, m) => {
      m.token != 20
    })
    val endsPoints = resolver.resolve(service, 19).selectedReplicas
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

  test("outgoing messages are assigned destination") {
    val tokenValue = 42
    val action = new Action("test", (message: InMessage) => {})
    action.supportedBy(service)
    val resolver = new Resolver(tokenExtractor = (actionPath: ActionPath, path: String) => tokenValue)

    val outMessage = new OutMessage()
    resolver.handleOutgoing(action, outMessage)
    assert(outMessage.destination.selectedReplicas.size > 0)
  }
}
