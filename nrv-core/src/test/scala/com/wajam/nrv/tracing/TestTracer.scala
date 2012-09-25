package com.wajam.nrv.tracing

import org.scalatest.{BeforeAndAfter, FunSuite}
import org.scalatest.matchers.ShouldMatchers._
import com.wajam.nrv.tracing.Annotation.Message
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._
import com.wajam.nrv.utils.{ControlableSequentialStringIdGenerator, ControlableCurrentTime}

/**
 *
 */
class TestTracer extends FunSuite with BeforeAndAfter with MockitoSugar {

  val mockRecorder: TraceRecorder = mock[TraceRecorder]
  val idGenerator = new ControlableSequentialStringIdGenerator {}
  val time = new ControlableCurrentTime {}
  val tracer = new Tracer(mockRecorder, time, idGenerator)

  before {
    reset(mockRecorder)
  }

  test("Should fail when context has no traceId") {
    val e = evaluating {
      TraceContext(null, "SID", None)
    } should produce [NullPointerException]
    e.getMessage should include ("traceId")
  }

  test("Should fail when context has no spanId") {
    val e = evaluating {
      TraceContext("TID", null, Some("PID"))
    } should produce [NullPointerException]
    e.getMessage should include ("spanId")
  }

  test("Should create a new tracing context if no current context") {

    tracer.currentContext should be (None)

    var called = false
    tracer.trace() {
      tracer.currentContext should not be (None)
      val context: TraceContext = tracer.currentContext.get
      context.traceId should not be (None)
      context.spanId should not be (None)
      context.parentSpanId should be (None)
      called = true
    }
    called should be (true)

    tracer.currentContext should be (None)
  }

  test("Should use specified tracing context if no current context") {

    tracer.currentContext should be (None)

    val context = TraceContext("TID", "SID", Some("PID"))

    var called = false
    tracer.trace(Some(context)) {
      tracer.currentContext should not be (None)
      tracer.currentContext should be (Some(context))
      context.parentSpanId should not be (None)
      called = true
    }
    called should be (true)

    tracer.currentContext should be (None)
  }

  test("Should create a child tracing context if a current context already exist") {

    tracer.currentContext should be (None)

    var called = false
    var childCalled = false
    tracer.trace() {

      tracer.currentContext should not be (None)

      val parent = tracer.currentContext
      tracer.trace() {
        tracer.currentContext should not be (Some(parent))

        tracer.currentContext.get.traceId should be (parent.get.traceId)
        tracer.currentContext.get.spanId should not be (None)
        tracer.currentContext.get.spanId should not be (parent.get.spanId)
        tracer.currentContext.get.parentSpanId should be (Some(parent.get.spanId))
        childCalled = true
      }

      tracer.currentContext should be (parent)
      called = true
    }

    tracer.currentContext should be (None)
    called should be (true)
    childCalled should be (true)

  }

  test("Should fail if new trace context isn't a child of parent context") {

    tracer.currentContext should be (None)

    var called = false
    var childCalled = false
    tracer.trace() {

      tracer.currentContext should not be (None)

      val parent = tracer.currentContext
      evaluating {
        val child = TraceContext(idGenerator.createId, idGenerator.createId, None)
        tracer.trace(Some(child)) {
          childCalled = true
        }
      } should produce [IllegalArgumentException]

      tracer.currentContext should be (parent)
      called = true
    }

    tracer.currentContext should be (None)
    called should be (true)
    childCalled should be (false)
  }

  test("Time should fail outside of a trace context") {

    var called = false
    evaluating {
      tracer.time("I'm outside a trace context!") {
        called = true
      }
    } should produce [IllegalStateException]
    called should be (false)

    verifyZeroInteractions(mockRecorder)
  }

  test("Time should be recorded") {

    val message: Message = Message("I'm in a context!")
    var context: Option[TraceContext] = None
    val duration = 1000

    var called = false
    tracer.trace() {
      context = tracer.currentContext
      tracer.time(message.content) {
        called = true
        time.currentTime += duration
      }
    }

    called should be (true)
    verify(mockRecorder).record(Record(context.get, time.currentTime, message, Some(duration)))
  }

  test("Record should fail outside of a trace context") {

    evaluating {
      tracer.record(Message("I'm outside a trace context!"))
    } should produce [IllegalStateException]

    verifyZeroInteractions(mockRecorder)
  }

  test("Record should record!") {

    val message: Message = Message("I'm in a context!")
    var context: Option[TraceContext] = None

    tracer.trace() {
      context = tracer.currentContext
      tracer.record(message)
    }

    verify(mockRecorder).record(Record(context.get, time.currentTime, message))
  }
}
