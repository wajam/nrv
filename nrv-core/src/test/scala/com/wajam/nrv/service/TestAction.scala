package com.wajam.nrv.service

import org.scalatest.{BeforeAndAfter, FunSuite}
import com.wajam.nrv.protocol.DummyProtocol
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import java.lang.String
import com.wajam.nrv.cluster.{StaticClusterManager, Node, Cluster}
import com.wajam.nrv.utils.Sync
import com.wajam.nrv.{TimeoutException, RemoteException, InvalidParameter}
import com.wajam.nrv.data.{OutMessage, InMessage}

@RunWith(classOf[JUnitRunner])
class TestAction extends FunSuite with BeforeAndAfter {

  var cluster: Cluster = null
  var service: Service = null

  before {
    cluster = new Cluster(new Node("127.0.0.1", Map("nrv" -> 12345, "dummy" -> 12346)), new StaticClusterManager)
    cluster.registerProtocol(new DummyProtocol("dummy", cluster), default = true)
    service = cluster.registerService(new Service("test", resolver = Some(new Resolver(1))))
    service.addMember(0, cluster.localNode)
  }

  after {
    service.stop()
  }

  test("call, reply") {
    var syncCall = new Sync[String]
    var syncResponse = new Sync[String]

    val action = service.registerAction(new Action("/test/:param", req => {
      req.parameters.getOrElse("call_key", "") match {
        case s: String =>
          syncCall.done(s)
        case _ =>
          syncCall.error(new Exception("Expected paramter 'call_key'"))
      }

      req.reply(Seq("response_key" -> "response_value"))
    }))
    action.start()

    action.tracer.trace() {
      action.call(Map("call_key" -> "call_value", "param" -> "param_value"), onReply = (resp, err) => {
        resp.parameters.getOrElse("response_key", "") match {
          case s: String =>
            syncResponse.done(s)
          case _ =>
            syncResponse.error(new Exception("Expected paramter 'response_key'"))
        }
      })
    }

    syncCall.thenWait(value => {
      assert(value == "call_value", "expected 'call_value', got '" + value + "'")
    }, 1000)

    syncResponse.thenWait(value => {
      assert(value == "response_value", "expected 'response_key', got '" + value + "'")
    }, 1000)

    action.stop()
  }

  test("call error") {
    var syncResponse = new Sync[InMessage]

    val action = service.registerAction(new Action("/test_error", req => {
      throw new InvalidParameter("TEST ERROR")
    }))
    action.start()

    action.tracer.trace() {
      action.call(Map("call_key" -> "call_value"), onReply = syncResponse.done(_, _))
    }

    val except = intercept[RemoteException] {
      syncResponse.thenWait(value => {
        fail("Shouldn't be call because an exception occured")
      }, 1000)
    }

    assert("Exception calling action implementation".equals(except.getMessage))
    assert("TEST ERROR".equals(except.getCause.getMessage))
    action.stop()
  }

  test("call timeout") {
    var syncResponse = new Sync[InMessage]

    val action = service.registerAction(new Action("/test_timeout", req => {
      // no reply, make it timeout
    }))

    action.start()

    val req = new OutMessage(Map("call_key" -> "call_value"), onReply = syncResponse.done(_, _))
    req.timeoutTime = 100

    action.tracer.trace() {
      action.call(req)
    }

    action.switchboard.getTime = ()=>{System.currentTimeMillis() + 100}
    action.switchboard.checkTimeout()

    intercept[TimeoutException] {
      syncResponse.thenWait(value => {
        fail("Shouldn't be call because an exception occured")
      }, 1000)
    }
    action.stop()
  }

  test("parameter in the path should be in the message parameters") {
    var syncCall = new Sync[String]

    val action = service.registerAction(new Action("/test/:param", req => {
      syncCall.done(req.parameters("param").asInstanceOf[String])
    }))
    action.start()

    val message = new InMessage()
    message.path = "/test/1"
    message.protocolName = "dummy"

    action.callIncomingHandlers(message)

    assert("1".equals(syncCall.get(1000)))

    action.stop()
  }
}
