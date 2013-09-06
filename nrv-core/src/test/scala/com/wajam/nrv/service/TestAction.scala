package com.wajam.nrv.service

import org.scalatest.{BeforeAndAfter, FunSuite}
import com.wajam.nrv.protocol.DummyProtocol
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import java.lang.String
import com.wajam.nrv.cluster.{LocalNode, StaticClusterManager, Cluster}
import com.wajam.nrv.{TimeoutException, RemoteException, InvalidParameter}
import com.wajam.nrv.data.{MString, OutMessage, InMessage}
import scala.concurrent.{Future, Promise, Await}
import scala.concurrent.duration._

import com.wajam.nrv.data.MValue._

@RunWith(classOf[JUnitRunner])
class TestAction extends FunSuite with BeforeAndAfter {

  var cluster: Cluster = null
  var service: Service = null

  before {
    val node = new LocalNode("127.0.0.1", Map("nrv" -> 12345, "dummy" -> 12346))
    cluster = new Cluster(node, new StaticClusterManager)
    cluster.registerProtocol(new DummyProtocol("dummy", node), default = true)
    service = cluster.registerService(new Service("test", new ActionSupportOptions(resolver = Some(new Resolver(1)))))
    val member = service.addMember(new ServiceMember(0, cluster.localNode))
    member.setStatus(MemberStatus.Up, triggerEvent = false)
  }

  after {
    service.stop()
  }

  test("call, reply") {
    val syncCall = Promise[String]

    val action = service.registerAction(new Action("/test/:param", req => {
      req.parameters.getOrElse("call_key", "") match {
        case MString(s) =>
          syncCall.success(s)
        case _ =>
          syncCall.failure(new Exception("Expected paramter 'call_key'"))
      }

      req.reply(Seq("response_key" -> "response_value"))
    }))
    action.start()

    val syncResponse: Future[InMessage] = action.tracer.trace() {
      action.call(Map("call_key" -> "call_value", "param" -> "param_value"))
    }

    val value = Await.result(syncCall.future, 1.second)
    assert(value == "call_value", "expected 'call_value', got '" + value + "'")

    value = Await.result(syncResponse, 1.second).parameters.getOrElse("response_key", "") match {
      case MString(s) => s
      case _ => fail("Expected parameter 'response_key'")
    }
    assert(value == "response_value", "expected 'response_key', got '" + value + "'")

    action.stop()
  }

  test("call error") {

    val action = service.registerAction(new Action("/test_error", req => {
      throw new InvalidParameter("TEST ERROR")
    }))
    action.start()

    val syncResponse = action.tracer.trace() {
      action.call(Map("call_key" -> "call_value"))
    }

    val except = intercept[RemoteException] {
      Await.result(syncResponse, 1.second)
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


    def completePromise(value: InMessage, optException: Option[Exception]) {
      optException match {
        case Some(e) => syncResponse.failure(e)
        case None => syncResponse.success(value)
      }
    }

    val req = new OutMessage(Map("call_key" -> "call_value"), onReply = completePromise, responseTimeout = 100)

    action.tracer.trace() {
      action.call(req)
    }

    action.switchboard.getTime = () => {
      System.currentTimeMillis() + 100
    }
    action.switchboard.checkTimeout()

    val start = System.currentTimeMillis()
    intercept[TimeoutException] {
      Await.result(syncResponse.future, 10.seconds)
    }
    assert((System.currentTimeMillis() - start) < 500)

    action.stop()
  }

  test("action default timeout") {

    val action = service.registerAction(new Action("/test_timeout", req => {
      // no reply, make it timeout
    }, actionSupportOptions = new ActionSupportOptions(responseTimeout = Some(100L))))

    action.start()

    val syncResponse = action.tracer.trace() {
      action.call(Map("call_key" -> "call_value"))
    }

    action.switchboard.getTime = () => {
      System.currentTimeMillis() + 100
    }
    action.switchboard.checkTimeout()

    val start = System.currentTimeMillis()
    intercept[TimeoutException] {
      Await.result(syncResponse, 10.seconds)
    }
    assert((System.currentTimeMillis() - start) < 500)

    action.stop()
  }

  test("action call method timeout") {

    val action = service.registerAction(new Action("/test_timeout", req => {
      // no reply, make it timeout
    }))

    action.start()

    val syncResponse = action.tracer.trace() {
      action.call(Map("call_key" -> "call_value"), responseTimeout = 100)
    }

    action.switchboard.getTime = () => {
      System.currentTimeMillis() + 100
    }
    action.switchboard.checkTimeout()

    val start = System.currentTimeMillis()
    intercept[TimeoutException] {
      Await.result(syncResponse, 10.seconds)
    }
    assert((System.currentTimeMillis() - start) < 500)

    action.stop()
  }

  test("parameter in the path should be in the message parameters") {
    val syncCall = Promise[String]

    val action = service.registerAction(new Action("/test/:param", req => {
      syncCall.success(req.parameters("param").asInstanceOf[MString].value)
    }))
    action.start()

    val message = new InMessage()
    message.path = "/test/1"
    message.protocolName = "dummy"

    action.callIncomingHandlers(message)

    assert("1".equals(Await.result(syncCall.future, 1.second)))

    action.stop()
  }

  test("call() should add the action method in the OutMessage") {

    val method = ActionMethod.POST
    var interceptedOutMessage: OutMessage = null

    val action = service.registerAction(new Action("/test/:param", req => {}, method) {

      override protected[nrv] def callOutgoingHandlers(outMessage: OutMessage) {
        interceptedOutMessage = outMessage
      }
    })
    action.start()

    action.call(new OutMessage(Map("call_key" -> "call_value")))

    assert(method === interceptedOutMessage.method)
  }
}
