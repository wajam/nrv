package com.wajam.nrv.service

import org.scalatest.FunSuite
import com.wajam.nrv.protocol.DummyProtocol
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import java.lang.String
import com.wajam.nrv.cluster.{StaticClusterManager, Node, Cluster}

@RunWith(classOf[JUnitRunner])
class TestAction extends FunSuite {
  val cluster = new Cluster(new Node("127.0.0.1", Map("nrv" -> 12345, "dummy" -> 12346)), new StaticClusterManager)
  cluster.registerProtocol(new DummyProtocol(cluster, "dummy"), default = true)
  val service = cluster.addService(new Service("test", resolver = Some(new Resolver(Some(1)))))

  test("call") {
    val notifier = new Object()
    var called = false
    var testValue: String = "NOTSET"

    val action = service.bind("/test", new Action(req => {
      called = true

      req.getOrElse("test", "") match {
        case s: String =>
          testValue = s

      }

      notifier.synchronized {
        notifier.notify()
      }
    }))

    action.call("test" -> "myvalue")()

    notifier.synchronized {
      notifier.wait(1)
      assert(called, "didn't received called action")
      assert(testValue == "myvalue", "expected 'test', got '" + testValue + "'")
    }
  }
}
