package com.wajam.nrv.cluster

import org.scalatest.FunSuite
import org.scalatest.mock.MockitoSugar
import org.mockito.Matchers._
import org.mockito.Mockito._
import com.wajam.nrv.service.Action
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import com.wajam.nrv.data.{MessageType, InRequest, OutRequest}

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
    inReq.function = MessageType.FUNCTION_RESPONSE
    inReq.rendezvous = outReq.rendezvous
    router !? inReq

    verify(mockAction).handleIncomingRequest(inReq, Some(outReq))
  }

}
