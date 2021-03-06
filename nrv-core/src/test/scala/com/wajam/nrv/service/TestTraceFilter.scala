package com.wajam.nrv.service

import org.scalatest.{BeforeAndAfter, FunSuite}
import org.scalatest.mock.MockitoSugar
import com.wajam.nrv.cluster.{LocalNode, StaticClusterManager, Cluster}
import com.wajam.nrv.protocol.DummyProtocol
import com.wajam.tracing._
import com.wajam.commons.{InetUtils, ControlableCurrentTime, ControlableSequentialStringIdGenerator}
import org.mockito.Mockito._
import org.mockito.Matchers._
import com.wajam.nrv.data.{MessageType, InMessage, OutMessage}
import org.mockito.ArgumentMatcher
import org.hamcrest.Description
import org.scalatest.Matchers._
import java.net.InetSocketAddress
import java.text.SimpleDateFormat
import com.wajam.tracing.Annotation._
import com.wajam.tracing.RpcName
import com.wajam.tracing.TraceContext
import com.wajam.tracing.Record
import scala.concurrent.{Promise, Await}
import scala.concurrent.duration._

/**
 *
 */
class TestTraceFilter extends FunSuite with BeforeAndAfter with MockitoSugar {

  val mockRecorder: TraceRecorder = mock[TraceRecorder]
  val idGenerator = new ControlableSequentialStringIdGenerator {}
  val time = new ControlableCurrentTime {}
  val tracer = new Tracer(mockRecorder, time, idGenerator)
  var cluster: Cluster = null
  var service: Service = null

  def setupCluster(nodeHost: String = "127.0.0.1") {
    idGenerator.reset
    reset(mockRecorder)
    val node = new LocalNode(nodeHost, Map("nrv" -> 12345, "dummy" -> 12346))
    cluster = new Cluster(node, new StaticClusterManager, new ActionSupportOptions(tracer = Some(tracer)))
    cluster.registerProtocol(new DummyProtocol("dummy", node), default = true)
    service = cluster.registerService(new Service("test", new ActionSupportOptions(resolver = Some(new Resolver(1)))))
    val member = service.addMember(new ServiceMember(0, cluster.localNode))
    member.setStatus(MemberStatus.Up, triggerEvent = false)
  }

  before {
    setupCluster()
  }

  after {
    service.stop()
  }

  class RecordMatcher(annClass: Class[_ <: Annotation], context: Option[TraceContext], timestamp: Long) extends ArgumentMatcher {

    def matches(argument: Any): Boolean = {
      val record = argument.asInstanceOf[Record]
      !(annClass != record.annotation.getClass || record.timestamp != timestamp ||
        (context.isDefined && context.get != record.context))
    }

    override def describeTo(description: Description) {
      super.describeTo(description)
      description.appendValue(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(timestamp))
      description.appendValue(annClass)
      description.appendValue(context)
    }
  }

  def matchRecord(annClass: Class[_ <: Annotation],
                  context: TraceContext = null,
                  timestamp: Long = time.currentTime) = {
    new RecordMatcher(annClass, Option(context), timestamp)
  }

  test("Should record incoming request with a new trace context when no context is present in message metadata") {

    val action = service.registerAction(new Action("/test1", (req) => Unit))
    val message = new InMessage()
    message.protocolName = "dummy"
    message.serviceName = service.name

    var called = false
    TraceFilter.handleIncoming(action, message, () => called = true)

    val expectedContext = TraceContext("0", "1", None, Some(true))
    val expectedRpcName = RpcName(service.name, "dummy", "", "/test1")
    verify(mockRecorder).record(Record(expectedContext, time.currentTime, ServerRecv(expectedRpcName)))
    verify(mockRecorder).record(argThat(matchRecord(classOf[ServerAddress])))
  }

  test("Should respect 'sampled' flag if it is present without other trace values in message metadata") {

    val action = service.registerAction(new Action("/test1", (req) => Unit))
    val message = new InMessage()
    message.protocolName = "dummy"
    message.serviceName = service.name
    message.metadata(TraceHeader.Sampled.toString) = true.toString

    var called = false
    TraceFilter.handleIncoming(action, message, () => called = true)

    val expectedContext = TraceContext("0", "1", None, Some(true))
    val expectedRpcName = RpcName(service.name, "dummy", "", "/test1")
    verify(mockRecorder).record(Record(expectedContext, time.currentTime, ServerRecv(expectedRpcName)))
    verify(mockRecorder).record(argThat(matchRecord(classOf[ServerAddress])))
  }

  test("Should adopt incoming request trace context when present in the message metadata") {

    val action = service.registerAction(new Action("/test1", (req) => Unit))
    val message = new InMessage()
    message.protocolName = "dummy"
    message.serviceName = service.name

    val expectedContext = TraceContext("TID", "SID", None, Some(true))
    TraceFilter.setContextInMessageMetadata(message, Some(expectedContext))
    var called = false
    TraceFilter.handleIncoming(action, message, () => called = true)

    val expectedRpcName = RpcName(service.name, "dummy", "", "/test1")
    called should be(true)
    verify(mockRecorder).record(Record(expectedContext, time.currentTime, ServerRecv(expectedRpcName)))
    verify(mockRecorder).record(argThat(matchRecord(classOf[ServerAddress])))
  }

  test("Should respect 'sampled' flag when present in the message metadata with the adopted context") {

    val action = service.registerAction(new Action("/test1", (req) => Unit))
    val message = new InMessage()
    message.protocolName = "dummy"
    message.serviceName = service.name

    val expectedContext = TraceContext("TID", "SID", None, Some(false))
    TraceFilter.setContextInMessageMetadata(message, Some(expectedContext))
    var called = false
    TraceFilter.handleIncoming(action, message, () => called = true)

    val expectedRpcName = RpcName(service.name, "dummy", "", "/test1")
    called should be(true)
    verify(mockRecorder, never()).record(Record(expectedContext, time.currentTime, ServerRecv(expectedRpcName)))
    verify(mockRecorder, never()).record(argThat(matchRecord(classOf[ServerAddress])))
  }

  test("Should record incoming response with matching out message trace context") {

    val action = service.registerAction(new Action("/test1", (req) => Unit))
    val message = new InMessage()
    message.function = MessageType.FUNCTION_RESPONSE

    val originalContext = TraceContext("TID", "SID", None, Some(true))
    val clientRecvContext = TraceContext("TID", "0", Some("SID"), Some(true))
    val outMessage = new OutMessage()
    outMessage.attachments(TraceHeader.OriginalContext) = originalContext
    TraceFilter.setContextInMessageMetadata(outMessage, Some(clientRecvContext))
    message.matchingOutMessage = Some(outMessage)

    var called = false
    TraceFilter.handleIncoming(action, message, () => {
      time.advanceTime(123)
      tracer.record(Annotation.Message("Got the response!"))
      called = true
    })

    called should be(true)
    verify(mockRecorder).record(Record(clientRecvContext, time.currentTime - 123, ClientRecv(Some(200))))
    verify(mockRecorder).record(Record(originalContext, time.currentTime, Annotation.Message("Got the response!")))
  }

  test("Should not record incoming response without matching request (i.e. response after timeout)") {

    val action = service.registerAction(new Action("/test1", (req) => Unit))
    val message = new InMessage()
    message.function = MessageType.FUNCTION_RESPONSE

    var called = false
    TraceFilter.handleIncoming(action, message, () => called = true)

    called should be(true)
    verifyZeroInteractions(mockRecorder)
  }

  test("Should record outgoing request with current trace context (new child context inherited from current)") {

    val action = service.registerAction(new Action("/test1", (req) => Unit))
    val message = new OutMessage()
    message.protocolName = "dummy"
    message.serviceName = service.name

    var called = false
    tracer.trace(Some(TraceContext("TID", "SID", None, Some(true)))) {
      TraceFilter.handleOutgoing(action, message, () => called = true)
    }

    val expectedContext = TraceContext("TID", "0", Some("SID"), Some(true))
    val expectedRpcName = RpcName(service.name, "dummy", "", "/test1")
    called should be(true)
    verify(mockRecorder).record(Record(expectedContext, time.currentTime, ClientSend(expectedRpcName)))
    verify(mockRecorder).record(argThat(matchRecord(classOf[ClientAddress])))
  }

  test("Should inherit 'sampled' flag in outgoing request subcontext") {

    val action = service.registerAction(new Action("/test1", (req) => Unit))
    val message = new OutMessage()
    message.protocolName = "dummy"
    message.serviceName = service.name

    var called = false
    tracer.trace(Some(TraceContext("TID", "SID", None, Some(true)))) {
      TraceFilter.handleOutgoing(action, message, () => called = true)
    }

    val expectedContext = TraceContext("TID", "0", Some("SID"), Some(true))
    val expectedRpcName = RpcName(service.name, "dummy", "", "/test1")
    called should be(true)
    verify(mockRecorder).record(Record(expectedContext, time.currentTime, ClientSend(expectedRpcName)))
    verify(mockRecorder).record(argThat(matchRecord(classOf[ClientAddress])))
  }

  test("Should record outgoing request outside a trace context in a brand new context (percolation)") {

    val action = service.registerAction(new Action("/test1", (req) => Unit))
    val message = new OutMessage()
    message.protocolName = "dummy"
    message.serviceName = service.name

    var called = false
    TraceFilter.handleOutgoing(action, message, () => called = true)

    val expectedContext = TraceContext("0", "1", None, Some(true))
    val expectedRpcName = RpcName(service.name, "dummy", "", "/test1")
    called should be(true)
    verify(mockRecorder).record(Record(expectedContext, time.currentTime, ClientSend(expectedRpcName)))
    verify(mockRecorder).record(argThat(matchRecord(classOf[ClientAddress])))
  }

  test("Should record outgoing response when inside a trace context (use current context)") {

    val action = service.registerAction(new Action("/test1", (req) => Unit))
    val message = new OutMessage()
    message.function = MessageType.FUNCTION_RESPONSE
    message.protocolName = "dummy"
    message.code = 201

    val expectedContext: TraceContext = TraceContext("TID", "SID", None, Some(true))
    var called = false
    tracer.trace(Some(expectedContext)) {
      TraceFilter.handleOutgoing(action, message, () => called = true)
    }

    called should be(true)
    verify(mockRecorder).record(Record(expectedContext, time.currentTime, ServerSend(Some(201))))
  }

  test("Should ignore outgoing response outside a trace context") {

    val action = service.registerAction(new Action("/test1", (req) => Unit))
    val message = new OutMessage()
    message.function = MessageType.FUNCTION_RESPONSE
    message.protocolName = "dummy"

    var called = false
    TraceFilter.handleOutgoing(action, message, () => called = true)

    called should be(true)
    verifyZeroInteractions(mockRecorder)
  }

  test("Should record local node address when not 'any' local address (i.e. not 0.0.0.0)") {

    val action = service.registerAction(new Action("/test1", (req) => Unit))
    val message = new InMessage()
    message.protocolName = "dummy"

    var called = false
    TraceFilter.handleIncoming(action, message, () => called = true)

    val expectedContext: TraceContext = TraceContext("0", "1", None, Some(true))
    val expectedAddress = ServerAddress(new InetSocketAddress(InetUtils.firstInetAddress.get.getHostName, 12346))

    called should be(true)
    verify(mockRecorder).record(argThat(matchRecord(classOf[ServerRecv])))
    verify(mockRecorder).record(Record(expectedContext, time.currentTime, expectedAddress))
  }

  test("Should record nested calls/replies in proper trace context") {
    val syncResponse = Promise[String]

    val originalTime = time.currentTime
    val childAction = service.registerAction(new Action("/child", req => {
      time.advanceTime(100)
      Tracer.currentTracer.get.record(Annotation.Message("child"))
      req.reply(Map())
    }))
    childAction.start()

    val parentAction = service.registerAction(new Action("/parent", req => {
      time.advanceTime(100)
      Tracer.currentTracer.get.record(Annotation.Message("parent before"))
      childAction.call(Map(), onReply = (resp, err) => {
        if (err.isEmpty) {
          time.advanceTime(100)
          Tracer.currentTracer.get.record(Annotation.Message("parent after"))
          req.reply(Map())
        }
      })
    }))
    parentAction.start()

    parentAction.call(Map(), onReply = (resp, err) => {
      if (err.isEmpty)
        syncResponse.success("OK")
    })

    Await.result(syncResponse.future, 1.second) should be("OK")

    parentAction.stop()
    childAction.stop()

    val parentContext = TraceContext("0", "1", None, Some(true))
    val childContext = TraceContext("0", "2", Some("1"), Some(true))
    verify(mockRecorder).record(argThat(matchRecord(classOf[ClientSend], parentContext, originalTime)))
    verify(mockRecorder).record(argThat(matchRecord(classOf[ClientAddress], parentContext, originalTime)))
    verify(mockRecorder).record(argThat(matchRecord(classOf[ServerRecv], parentContext, originalTime)))
    verify(mockRecorder).record(argThat(matchRecord(classOf[ServerAddress], parentContext, originalTime)))
    verify(mockRecorder).record(Record(parentContext, originalTime + 100, Annotation.Message("parent before")))
    verify(mockRecorder).record(argThat(matchRecord(classOf[ClientSend], childContext, originalTime + 100)))
    verify(mockRecorder).record(argThat(matchRecord(classOf[ClientAddress], childContext, originalTime + 100)))
    verify(mockRecorder).record(argThat(matchRecord(classOf[ServerRecv], childContext, originalTime + 100)))
    verify(mockRecorder).record(argThat(matchRecord(classOf[ServerAddress], childContext, originalTime + 100)))
    verify(mockRecorder).record(Record(childContext, originalTime + 200, Annotation.Message("child")))
    verify(mockRecorder).record(argThat(matchRecord(classOf[ServerSend], childContext, originalTime + 200)))
    verify(mockRecorder).record(argThat(matchRecord(classOf[ClientRecv], childContext, originalTime + 200)))
    verify(mockRecorder).record(Record(parentContext, originalTime + 300, Annotation.Message("parent after")))
    verify(mockRecorder).record(argThat(matchRecord(classOf[ServerSend], parentContext, originalTime + 300)))
    verify(mockRecorder).record(argThat(matchRecord(classOf[ClientRecv], parentContext, originalTime + 300)))
  }
}
