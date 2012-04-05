package com.wajam.nrv.cluster

import org.scalatest.FunSuite
import org.scalatest.mock.MockitoSugar
import org.mockito.Matchers._
import org.mockito.Mockito._
import com.wajam.nrv.service.Action
import com.wajam.nrv.data.{InRequest, OutRequest}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TestRouter extends FunSuite with MockitoSugar {

  test("routing") {
    val mockCluster = mock[Cluster]
    val mockAction = mock[Action]

    when(mockCluster.getAction(anyObject())) thenReturn mockAction

    var router = new Router(mockCluster)
    router.start()

    val outReq = new OutRequest()
    outReq.path = "/test"
    router !? outReq

    val inReq = new InRequest()
    inReq.rendezvous = outReq.rendezvous
    router !? inReq

    verify(mockAction).handleIncomingRequest(inReq, Some(outReq))

  }

}
