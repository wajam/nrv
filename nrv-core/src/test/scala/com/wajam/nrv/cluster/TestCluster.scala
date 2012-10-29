package com.wajam.nrv.cluster

import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import scala.Predef._
import com.wajam.nrv.service._

@RunWith(classOf[JUnitRunner])
class TestCluster extends FunSuite {
  val cluster = new Cluster(new Node("127.0.0.1", Map("nrv" -> 12345, "dummy" -> 12346)), new StaticClusterManager)

  val method = "testmethod"
  val srv1 = cluster.registerService(new Service("test1"))
  val srv2 = cluster.registerService(new Service("test2"))

  var act1: Action = null
  var act2: Action = null
  var act3: Action = null

  test("add service") {
    assert(cluster.services.size == 2)

    act1 = srv1.registerAction(new Action("/testabc", req => Unit))
    act2 = srv1.registerAction(new Action("/testdef", req => Unit, method))
    act3 = srv2.registerAction(new Action("/testabc", req => Unit, method))
  }

  test("url resolving") {
    val resolvedAct1 = cluster.getAction(new ActionURL(srv1.name, "/testabc"), ActionMethod.ANY)
    assert(resolvedAct1 == act1, "Got " + resolvedAct1)

    val resolvedNonExisting = cluster.getAction(new ActionURL(srv1.name, "/testabc123"), ActionMethod.ANY)
    assert(resolvedNonExisting == null)

    val resolvedAct2 = cluster.getAction(new ActionURL(srv1.name, "/testdef"), ActionMethod.ANY)
    assert(resolvedAct2 == act2, "Got " + resolvedAct2)

    val resolvedAct3 = cluster.getAction(new ActionURL(srv2.name, "/testabc"), ActionMethod.ANY)
    assert(resolvedAct3 == act3, "Got " + resolvedAct3)
  }

  test("action registered with a method should resolve with ANY") {
    val resolvedAct = cluster.getAction(new ActionURL(srv2.name, "/testabc"), ActionMethod.ANY)
    assert(resolvedAct == act3, "Got " + resolvedAct)
  }

  test("action registered with ANY should resolve with a method") {
    val resolvedAct = cluster.getAction(new ActionURL(srv2.name, "/testabc"), ActionMethod(method))
    assert(resolvedAct == act3, "Got " + resolvedAct)
  }

  test("action registered with a method should resolve with the exact method") {
    val resolvedAct = cluster.getAction(new ActionURL(srv2.name, "/testabc"), ActionMethod(method))
    assert(resolvedAct == act3, "Got " + resolvedAct)
  }

  test("no action match with method") {
    val resolvedAct = cluster.getAction(new ActionURL(srv2.name, "/testabc"), ActionMethod("other"))
    assert(resolvedAct == null)
  }
}
