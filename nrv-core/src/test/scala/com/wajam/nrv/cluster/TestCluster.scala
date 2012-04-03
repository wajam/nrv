package com.wajam.nrv.cluster

import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import com.wajam.nrv.service.{ActionUrl, Action, Service}

@RunWith(classOf[JUnitRunner])
class TestCluster extends FunSuite {
  val cluster = new Cluster(new Node("127.0.0.1", Map("nrv" -> 12345, "dummy" -> 12346)), new StaticClusterManager)

  val srv1 = cluster.addService(new Service("test1"))
  val srv2 = cluster.addService(new Service("test2"))

  var act1: Action = null
  var act2: Action = null
  var act3: Action = null

  test("add service") {
    assert(cluster.services.size == 2)

    act1 = srv1.bind("/testabc", new Action(req => Unit))
    act2 = srv1.bind("/testdef", new Action(req => Unit))
    act3 = srv2.bind("/testabc", new Action(req => Unit))
  }

  test("url resolving") {
    val resolvedAct1 = cluster.getAction(new ActionUrl(srv1.name, "/testabc"))
    assert(resolvedAct1 == act1, "Got " + resolvedAct1)

    val resolvedNonExisting = cluster.getAction(new ActionUrl(srv1.name, "/testabc123"))
    assert(resolvedNonExisting == null)

    val resolvedAct2 = cluster.getAction(new ActionUrl(srv1.name, "/testdef"))
    assert(resolvedAct2 == act2, "Got " + resolvedAct2)

    val resolvedAct3 = cluster.getAction(new ActionUrl(srv2.name, "/testabc"))
    assert(resolvedAct3 == act3, "Got " + resolvedAct3)
  }

}
