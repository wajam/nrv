package com.wajam.nrv.service

import org.scalatest.{BeforeAndAfter, FunSuite}
import org.scalatest.mock.MockitoSugar
import com.wajam.nrv.cluster.{StaticClusterManager, Node, Cluster}
import com.wajam.nrv.protocol.DummyProtocol
import com.wajam.nrv.tracing._
import com.wajam.nrv.utils.{Sync, ControlableSequentialStringIdGenerator, ControlableCurrentTime}
import org.mockito.Mockito._
import org.mockito.Matchers._
import com.wajam.nrv.data.{MessageType, InMessage, OutMessage}
import com.wajam.nrv.tracing.Annotation._
import org.mockito.ArgumentMatcher
import org.hamcrest.Description
import org.scalatest.matchers.ShouldMatchers._
import com.wajam.nrv.tracing.TraceContext
import com.wajam.nrv.tracing.Record
import java.net.{InetAddress, InetSocketAddress}
import java.text.SimpleDateFormat

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
    cluster = new Cluster(new Node(nodeHost, Map("nrv" -> 12345, "dummy" -> 12346)), new StaticClusterManager, tracer = tracer)
    cluster.registerProtocol(new DummyProtocol("dummy", cluster), default = true)
    service = cluster.registerService(new Service("test", resolver = Some(new Resolver(1))))
    service.addMember(0, cluster.localNode)
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
    TraceFilter.handleIncoming(action, message, _ => called = true)

    val expectedContext = TraceContext("0", "1", None)
    val expectedRpcName = RpcName(service.name, "dummy", "", "/test1")
    verify(mockRecorder).record(Record(expectedContext, time.currentTime, ServerRecv(expectedRpcName)))
    verify(mockRecorder).record(argThat(matchRecord(classOf[ServerAddress])))
  }

  test("Should adopt incoming request trace context when present in the message metadata") {

    val action = service.registerAction(new Action("/test1", (req) => Unit))
    val message = new InMessage()
    message.protocolName = "dummy"
    message.serviceName = service.name

    val expectedContext = TraceContext("TID", "SID", None)
    TraceFilter.setContextInMessageMetadata(message, Some(expectedContext))
    var called = false
    TraceFilter.handleIncoming(action, message, _ => called = true)

    val expectedRpcName = RpcName(service.name, "dummy", "", "/test1")
    called should be (true)
    verify(mockRecorder).record(Record(expectedContext, time.currentTime, ServerRecv(expectedRpcName)))
    verify(mockRecorder).record(argThat(matchRecord(classOf[ServerAddress])))
  }

  test("Should record incoming response with matching out message trace context") {

    val action = service.registerAction(new Action("/test1", (req) => Unit))
    val message = new InMessage()
    message.function = MessageType.FUNCTION_RESPONSE

    val originalContext = TraceContext("TID", "SID", None)
    val clientRecvContext = TraceContext("TID", "0", Some("SID"))
    val outMessage = new OutMessage()
    outMessage.attachments(TraceHeader.OriginalContext) = originalContext
    TraceFilter.setContextInMessageMetadata(outMessage, Some(clientRecvContext))
    message.matchingOutMessage = Some(outMessage)

    var called = false
    TraceFilter.handleIncoming(action, message, _ => {
      time.advanceTime(123)
      tracer.record(Annotation.Message("Got the response!"))
      called = true
    })

    called should be (true)
    verify(mockRecorder).record(Record(clientRecvContext, time.currentTime-123, ClientRecv(Some(200))))
    verify(mockRecorder).record(Record(originalContext, time.currentTime, Annotation.Message("Got the response!")))
  }

  test("Should not record incoming response without matching request (i.e. response after timeout)") {

    val action = service.registerAction(new Action("/test1", (req) => Unit))
    val message = new InMessage()
    message.function = MessageType.FUNCTION_RESPONSE

    var called = false
    TraceFilter.handleIncoming(action, message, _ => called = true)

    called should be (true)
    verifyZeroInteractions(mockRecorder)
  }

  test("Should record outgoing request with current trace context (new child context inherited from current)") {

    val action = service.registerAction(new Action("/test1", (req) => Unit))
    val message = new OutMessage()
    message.protocolName = "dummy"
    message.serviceName = service.name

    var called = false
    tracer.trace(Some(TraceContext("TID", "SID", None))) {
      TraceFilter.handleOutgoing(action, message, _ => called = true)
    }

    val expectedContext = TraceContext("TID", "0", Some("SID"))
    val expectedRpcName = RpcName(service.name, "dummy", "", "/test1")
    called should be (true)
    verify(mockRecorder).record(Record(expectedContext, time.currentTime, ClientSend(expectedRpcName)))
    verify(mockRecorder).record(argThat(matchRecord(classOf[ClientAddress])))
  }

  test("Should record outgoing request outside a trace context in a brand new context (percolation)") {

    val action = service.registerAction(new Action("/test1", (req) => Unit))
    val message = new OutMessage()
    message.protocolName = "dummy"
    message.serviceName = service.name

    var called = false
    TraceFilter.handleOutgoing(action, message, _ => called = true)

    val expectedContext = TraceContext("0", "1", None)
    val expectedRpcName = RpcName(service.name, "dummy", "", "/test1")
    called should be (true)
    verify(mockRecorder).record(Record(expectedContext, time.currentTime, ClientSend(expectedRpcName)))
    verify(mockRecorder).record(argThat(matchRecord(classOf[ClientAddress])))
  }

  test("Should record outgoing response when inside a trace context (use current context)") {

    val action = service.registerAction(new Action("/test1", (req) => Unit))
    val message = new OutMessage()
    message.function = MessageType.FUNCTION_RESPONSE
    message.protocolName = "dummy"
    message.code = 201

    val expectedContext: TraceContext = TraceContext("TID", "SID", None)
    var called = false
    tracer.trace(Some(expectedContext)) {
      TraceFilter.handleOutgoing(action, message, _ => called = true)
    }

    called should be (true)
    verify(mockRecorder).record(Record(expectedContext, time.currentTime, ServerSend(Some(201))))
  }

  test("Should ignore outgoing response outside a trace context") {

    val action = service.registerAction(new Action("/test1", (req) => Unit))
    val message = new OutMessage()
    message.function = MessageType.FUNCTION_RESPONSE
    message.protocolName = "dummy"

    var called = false
    TraceFilter.handleOutgoing(action, message, _ => called = true)

    called should be (true)
    verifyZeroInteractions(mockRecorder)
  }

  test("Should record local node address when not 'any' local address (i.e. not 0.0.0.0)") {

    val action = service.registerAction(new Action("/test1", (req) => Unit))
    val message = new InMessage()
    message.protocolName = "dummy"

    var called = false
    TraceFilter.handleIncoming(action, message, _ => called = true)

    val expectedContext: TraceContext = TraceContext("0", "1", None)
    val expectedAddress = ServerAddress(new InetSocketAddress("127.0.0.1", 12346))

    called should be (true)
    verify(mockRecorder).record(argThat(matchRecord(classOf[ServerRecv])))
    verify(mockRecorder).record(Record(expectedContext, time.currentTime, expectedAddress))
  }

  test("Should record first local network address when local node address is 0.0.0.0") {

    // Node host may be resolved in the future and not stay 'any local address' (i.e.'0.0.0.0') in the future but
    // this is the current behavior and we currently have to deal with it
    setupCluster("0.0.0.0")
    cluster.localNode.host should be (InetAddress.getByName("0.0.0.0"))

    val action = service.registerAction(new Action("/test1", (req) => Unit))
    val message = new InMessage()
    message.protocolName = "dummy"

    var called = false
    TraceFilter.handleIncoming(action, message, _ => called = true)

    called should be (true)
    verify(mockRecorder).record(argThat(matchRecord(classOf[ServerRecv])))
    verify(mockRecorder).record(argThat(new ArgumentMatcher {
      def matches(argument: Any) = {
        val record = argument.asInstanceOf[Record]
        if (record.annotation.isInstanceOf[ServerAddress]) {
          val address = record.annotation.asInstanceOf[ServerAddress]
          val hostAddress = address.addr.getAddress.getHostAddress
          hostAddress should not be ("0.0.0.0")
          hostAddress should fullyMatch regex """^(?:[0-9]{1,3}\.){3}[0-9]{1,3}$"""
          true
        } else {
          false
        }
      }
    }))
  }

  test("Should record nested calls/replies in proper trace context") {
    var syncResponse = new Sync[String]

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
        syncResponse.done("OK")
    })
    syncResponse.thenWait(response => {
      response should be ("OK")
    }, 1000)

    parentAction.stop()
    childAction.stop()

    val parentContext = TraceContext("0", "1", None)
    val childContext = TraceContext("0", "2", Some("1"))
    verify(mockRecorder).record(argThat(matchRecord(classOf[ClientSend], parentContext, originalTime)))
    verify(mockRecorder).record(argThat(matchRecord(classOf[ClientAddress], parentContext, originalTime)))
    verify(mockRecorder).record(argThat(matchRecord(classOf[ServerRecv], parentContext, originalTime)))
    verify(mockRecorder).record(argThat(matchRecord(classOf[ServerAddress], parentContext, originalTime)))
    verify(mockRecorder).record(Record(parentContext, originalTime + 100, Annotation.Message("parent before")))
    verify(mockRecorder).record(argThat(matchRecord(classOf[ClientSend], childContext, originalTime + 100)))
    verify(mockRecorder).record(argThat(matchRecord(classOf[ClientAddress], childContext, originalTime + 100)))
    verify(mockRecorder).record(argThat(matchRecord(classOf[ServerRecv], childContext ,originalTime + 100)))
    verify(mockRecorder).record(argThat(matchRecord(classOf[ServerAddress] ,childContext, originalTime + 100)))
    verify(mockRecorder).record(Record(childContext, originalTime + 200, Annotation.Message("child")))
    verify(mockRecorder).record(argThat(matchRecord(classOf[ServerSend], childContext, originalTime + 200)))
    verify(mockRecorder).record(argThat(matchRecord(classOf[ClientRecv], childContext, originalTime + 200)))
    verify(mockRecorder).record(Record(parentContext, originalTime + 300, Annotation.Message("parent after")))
    verify(mockRecorder).record(argThat(matchRecord(classOf[ServerSend], parentContext, originalTime + 300)))
    verify(mockRecorder).record(argThat(matchRecord(classOf[ClientRecv], parentContext, originalTime + 300)))
  }
}
