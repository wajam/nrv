package com.wajam.nrv.service

import org.scalatest.FunSuite
import com.wajam.nrv.protocol.DummyProtocol
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import java.lang.String
import com.wajam.nrv.cluster.{StaticClusterManager, Node, Cluster}
import com.wajam.nrv.utils.Sync
import com.wajam.nrv.{TimeoutException, UnavailableException, RemoteException, InvalidParameter}
import com.wajam.nrv.data.{OutMessage, InMessage}

@RunWith(classOf[JUnitRunner])
class TestAction extends FunSuite {
  val cluster = new Cluster(new Node("127.0.0.1", Map("nrv" -> 12345, "dummy" -> 12346)), new StaticClusterManager)
  cluster.registerProtocol(new DummyProtocol(cluster, "dummy"), default = true)
  val service = cluster.addService(new Service("test", resolver = Some(new Resolver(Some(1)))))
  service.addMember(0, cluster.localNode)

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


    action.call(Map("call_key" -> "call_value", "param" -> "param_value"), onReply = (resp, err) => {
      resp.parameters.getOrElse("response_key", "") match {
        case s: String =>
          syncResponse.done(s)
        case _ =>
          syncResponse.error(new Exception("Expected paramter 'response_key'"))
      }
    })

    syncCall.thenWait(value => {
      assert(value == "call_value", "expected 'call_value', got '" + value + "'")
    }, 100)

    syncResponse.thenWait(value => {
      assert(value == "response_value", "expected 'response_key', got '" + value + "'")
    }, 100)
  }

  test("call error") {
    var syncResponse = new Sync[InMessage]

    val action = service.registerAction(new Action("/test_error", req => {
      throw new InvalidParameter("TEST ERROR")
    }))

    action.call(Map("call_key" -> "call_value"), onReply = syncResponse.done(_, _))

    val except = intercept[RemoteException] {
      syncResponse.thenWait(value => {
        fail("Shouldn't be call because an exception occured")
      }, 1000)
    }

    assert(except.getMessage == "TEST ERROR")
  }

  test("call timeout") {
    var syncResponse = new Sync[InMessage]

    val action = service.registerAction(new Action("/test_timeout", req => {
      // no reply, make it timeout
    }))

    val req = new OutMessage(Map("call_key" -> "call_value"), onReply = syncResponse.done(_, _))
    req.timeoutTime = 100
    action.call(req)

    action.switchboard.getTime = ()=>{System.currentTimeMillis() + 100}
    action.switchboard.checkTimeout()

    intercept[TimeoutException] {
      syncResponse.thenWait(value => {
        fail("Shouldn't be call because an exception occured")
      }, 1000)
    }
  }

  test("parameter in the path should be in the message parameters") {
    var syncCall = new Sync[String]

    val action = service.registerAction(new Action("/test/:param", req => {
      syncCall.done(req.parameters("param").asInstanceOf[String])
    }))
    action.start()

    val message = new InMessage()
    message.path = "/test/1"

    action.callIncomingHandlers(message)

    assert("1".equals(syncCall.get(1000)))
  }
}
