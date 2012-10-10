package com.wajam.nrv.service

import org.scalatest.{BeforeAndAfter, FunSuite}
import com.wajam.nrv.protocol.DummyProtocol
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import java.lang.String
import com.wajam.nrv.cluster.{StaticClusterManager, Node, Cluster}
import com.wajam.nrv.{TimeoutException, RemoteException, InvalidParameter}
import com.wajam.nrv.data.{OutMessage, InMessage}
import com.wajam.nrv.utils.{Future, Promise}

@RunWith(classOf[JUnitRunner])
class TestAction extends FunSuite with BeforeAndAfter {

  var cluster: Cluster = null
  var service: Service = null

  before {
    cluster = new Cluster(new Node("127.0.0.1", Map("nrv" -> 12345, "dummy" -> 12346)), new StaticClusterManager)
    cluster.registerProtocol(new DummyProtocol("dummy", cluster), default = true)
    service = cluster.registerService(new Service("test", defaultResolver = Some(new Resolver(1))))
    val member = service.addMember(0, cluster.localNode)
    member.setStatus(MemberStatus.Up, triggerEvent = false)
  }

  after {
    service.stop()
  }

  test("call, reply") {
    val syncCall = Promise[String]
    val syncResponse = Promise[String]

    val action = service.registerAction(new Action("/test/:param", req => {
      req.parameters.getOrElse("call_key", "") match {
        case s: String =>
          syncCall.success(s)
        case _ =>
          syncCall.failure(new Exception("Expected paramter 'call_key'"))
      }

      req.reply(Seq("response_key" -> "response_value"))
    }))
    action.start()

    action.tracer.trace() {
      action.call(Map("call_key" -> "call_value", "param" -> "param_value"), onReply = (resp, err) => {
        resp.parameters.getOrElse("response_key", "") match {
          case s: String =>
            syncResponse.success(s)
          case _ =>
            syncResponse.failure(new Exception("Expected paramter 'response_key'"))
        }
      })
    }

    var value = Future.blocking(syncCall.future, 1000)
    assert(value == "call_value", "expected 'call_value', got '" + value + "'")

    value = Future.blocking(syncResponse.future, 1000)
    assert(value == "response_value", "expected 'response_key', got '" + value + "'")

    action.stop()
  }

  test("call error") {
    val syncResponse = Promise[InMessage]

    val action = service.registerAction(new Action("/test_error", req => {
      throw new InvalidParameter("TEST ERROR")
    }))
    action.start()

    action.tracer.trace() {
      action.call(Map("call_key" -> "call_value"), onReply = syncResponse.complete(_, _))
    }

    val except = intercept[RemoteException] {
      Future.blocking(syncResponse.future, 1000)
      fail("Shouldn't be call because an exception occured")
    }

    assert("Exception calling action implementation".equals(except.getMessage))
    assert("TEST ERROR".equals(except.getCause.getMessage))
    action.stop()
  }

  test("call timeout") {
    val syncResponse = Promise[InMessage]

    val action = service.registerAction(new Action("/test_timeout", req => {
      // no reply, make it timeout
    }))

    action.start()

    val req = new OutMessage(Map("call_key" -> "call_value"), onReply = syncResponse.complete(_, _))
    req.timeoutTime = 100

    action.tracer.trace() {
      action.call(req)
    }

    action.switchboard.getTime = () => {
      System.currentTimeMillis() + 100
    }
    action.switchboard.checkTimeout()

    intercept[TimeoutException] {
      Future.blocking(syncResponse.future, 1000)
      fail("Shouldn't be call because an exception occured")
    }
    action.stop()
  }

  test("parameter in the path should be in the message parameters") {
    val syncCall = Promise[String]

    val action = service.registerAction(new Action("/test/:param", req => {
      syncCall.success(req.parameters("param").asInstanceOf[String])
    }))
    action.start()

    val message = new InMessage()
    message.path = "/test/1"
    message.protocolName = "dummy"

    action.callIncomingHandlers(message)

    assert("1".equals(Future.blocking(syncCall.future, 1000)))

    action.stop()
  }
}
