package com.wajam.nrv.service

import org.scalatest.{BeforeAndAfter, FunSuite}
import org.scalatest.mock.MockitoSugar
import com.wajam.nrv.cluster.{StaticClusterManager, Node, Cluster}
import com.wajam.nrv.protocol.DummyProtocol
import com.wajam.nrv.tracing._
import com.wajam.nrv.utils.{ControlableSequencialStringIdGenerator, ControlableCurrentTime}
import org.mockito.Mockito._
import org.mockito.Matchers._
import com.wajam.nrv.data.{MessageType, InMessage, OutMessage}
import com.wajam.nrv.tracing.Annotation._
import org.mockito.ArgumentMatcher
import org.hamcrest.Description
import org.scalatest.matchers.ShouldMatchers._
import com.wajam.nrv.tracing.TraceContext
import com.wajam.nrv.tracing.Record
import scala.Some
import java.net.{InetAddress, InetSocketAddress}

/**
 *
 */
class TestTraceFilter extends FunSuite with BeforeAndAfter with MockitoSugar {

  val mockRecorder: TraceRecorder = mock[TraceRecorder]
  val idGenerator = new ControlableSequencialStringIdGenerator
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

  class RecordMatcher(annClass: Class[_ <: Annotation], timestamp: Long, context: Option[TraceContext]) extends ArgumentMatcher {

    def matches(argument: Any): Boolean = {
      val record = argument.asInstanceOf[Record]
      !(annClass != record.annotation.getClass || record.timestamp != timestamp ||
        (context.isDefined && context.get != record.context))
    }

    override def describeTo(description: Description) {
      super.describeTo(description)
      description.appendValue(timestamp)
      description.appendValue(annClass)
      description.appendValue(context)
    }
  }

  def matchRecord(annClass: Class[_ <: Annotation], timestamp: Long = time.currentTime,
                  context: Option[TraceContext] = None) = {
    new RecordMatcher(annClass, timestamp, context)
  }

  test("Should record incomming request without trace context (brand new context)") {

    val action = service.registerAction(new Action("/test1", (req) => Unit))
    val message = new InMessage()
    message.protocolName = "dummy"

    TraceFilter.handleIncoming(action, message)

    val expectedContext: TraceContext = TraceContext(Some("0"), None, Some("1"))
    verify(mockRecorder).record(Record(expectedContext, time.currentTime, ServerRecv()))
    verify(mockRecorder).record(argThat(matchRecord(classOf[RpcName])))
    verify(mockRecorder).record(argThat(matchRecord(classOf[ServerAddress])))
  }

  test("Should record incomming request with current trace context (new child context inherited from current)") {

    val action = service.registerAction(new Action("/test1", (req) => Unit))
    val message = new InMessage()
    message.protocolName = "dummy"

    tracer.trace(Some(TraceContext(Some("TID"), None, Some("SID")))) {
      TraceFilter.handleIncoming(action, message)
    }

    val expectedContext: TraceContext = TraceContext(Some("TID"), Some("SID"), Some("0"))
    verify(mockRecorder).record(Record(expectedContext, time.currentTime, ServerRecv()))
    verify(mockRecorder).record(argThat(matchRecord(classOf[RpcName])))
    verify(mockRecorder).record(argThat(matchRecord(classOf[ServerAddress])))
  }

  test("Should record incomming response with matching out message") {

    val action = service.registerAction(new Action("/test1", (req) => Unit))
    val message = new InMessage()
    message.function = MessageType.FUNCTION_RESPONSE

    val expectedContext: TraceContext = TraceContext(Some("TID"), None, Some("SID"))
    val outMessage = new OutMessage()
    TraceFilter.setMessageContext(outMessage, Some(expectedContext))
    message.matchingOutMessage = Some(outMessage)

    TraceFilter.handleIncoming(action, message)

    verify(mockRecorder).record(Record(expectedContext, time.currentTime, ClientRecv(Some(200))))
  }

  test("Should do nothing with incomming response without matching request (i.e. response after timeout)") {

    val action = service.registerAction(new Action("/test1", (req) => Unit))
    val message = new InMessage()
    message.function = MessageType.FUNCTION_RESPONSE

    TraceFilter.handleIncoming(action, message)

    verifyZeroInteractions(mockRecorder)
  }

  test("Should record outgoing request with current trace context (new child context inherited from current)") {

    val action = service.registerAction(new Action("/test1", (req) => Unit))
    val message = new OutMessage()
    message.protocolName = "dummy"

    tracer.trace(Some(TraceContext(Some("TID"), None, Some("SID")))) {
      TraceFilter.handleOutgoing(action, message)
    }

    val expectedContext: TraceContext = TraceContext(Some("TID"), Some("SID"), Some("0"))
    verify(mockRecorder).record(Record(expectedContext, time.currentTime, ClientSend()))
    verify(mockRecorder).record(argThat(matchRecord(classOf[RpcName])))
    verify(mockRecorder).record(argThat(matchRecord(classOf[ClientAddress])))
  }

  ignore("Should fail on outgoing request without current trace context") {

    val action = service.registerAction(new Action("/test1", (req) => Unit))
    val message = new OutMessage()
    message.protocolName = "dummy"

    evaluating {
      TraceFilter.handleOutgoing(action, message)
    } should produce [IllegalStateException]

    verifyZeroInteractions(mockRecorder)
  }

  test("Should record outgoing response when inside a trace context (use current context)") {

    val action = service.registerAction(new Action("/test1", (req) => Unit))
    val message = new OutMessage()
    message.function = MessageType.FUNCTION_RESPONSE
    message.protocolName = "dummy"
    message.code = 201

    val expectedContext: TraceContext = TraceContext(Some("TID"), None, Some("SID"))
    tracer.trace(Some(expectedContext)) {
      TraceFilter.handleOutgoing(action, message)
    }

    verify(mockRecorder).record(Record(expectedContext, time.currentTime, ServerSend(Some(201))))
  }

  ignore("Should fail on outgoing response without current trace context") {

    val action = service.registerAction(new Action("/test1", (req) => Unit))
    val message = new OutMessage()
    message.function = MessageType.FUNCTION_RESPONSE
    message.protocolName = "dummy"

    evaluating {
      TraceFilter.handleOutgoing(action, message)
    } should produce [IllegalStateException]

    verifyZeroInteractions(mockRecorder)
  }

  test("Should record local node address when not 'any' local address (i.e. not 0.0.0.0)") {

    val action = service.registerAction(new Action("/test1", (req) => Unit))
    val message = new InMessage()
    message.protocolName = "dummy"

    TraceFilter.handleIncoming(action, message)

    val expectedContext: TraceContext = TraceContext(Some("0"), None, Some("1"))
    val expectedAddress = ServerAddress(new InetSocketAddress("127.0.0.1", 12346))

    verify(mockRecorder).record(argThat(matchRecord(classOf[ServerRecv])))
    verify(mockRecorder).record(argThat(matchRecord(classOf[RpcName])))
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

    TraceFilter.handleIncoming(action, message)

    verify(mockRecorder).record(argThat(matchRecord(classOf[ServerRecv])))
    verify(mockRecorder).record(argThat(matchRecord(classOf[RpcName])))
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
}
