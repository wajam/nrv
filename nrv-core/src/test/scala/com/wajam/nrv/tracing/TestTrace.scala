package com.wajam.nrv.tracing

import org.scalatest.{BeforeAndAfter, FunSuite}
import org.scalatest.matchers.ShouldMatchers._
import com.wajam.nrv.tracing.Annotation.Message
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._
import com.wajam.nrv.utils.CurrentTime
import com.sun.scenario.animation.shared.CurrentTime

/**
 *
 */
class TestTrace extends FunSuite with BeforeAndAfter with MockitoSugar {

  test("Should fail when context has no traceId") {
    val e = evaluating {
      TraceContext(None)
    } should produce [IllegalArgumentException]
    e.getMessage should include ("traceId")
  }

  test("Should fail when context has no spanId") {
    val e = evaluating {
      TraceContext(TraceContext.createId, TraceContext.createId, None)
    } should produce [IllegalArgumentException]
    e.getMessage should include ("spanId")
  }

  test("Should create a new tracing context if no current context") {

    Trace.currentContext should be (None)

    var called = false
    Trace.trace() {
      Trace.currentContext should not be (None)
      val context: TraceContext = Trace.currentContext.get
      context.traceId should not be (None)
      context.spanId should not be (None)
      context.parentSpanId should be (None)
      called = true
    }
    called should be (true)

    Trace.currentContext should be (None)
  }

  test("Should use specified tracing context if no current context") {

    Trace.currentContext should be (None)

    val context = TraceContext(TraceContext.createId, TraceContext.createId, TraceContext.createId)

    var called = false
    Trace.trace(Some(context)) {
      Trace.currentContext should not be (None)
      Trace.currentContext should be (Some(context))
      context.parentSpanId should not be (None)
      called = true
    }
    called should be (true)

    Trace.currentContext should be (None)
  }

  test("Should create a child tracing context if a current context already exist") {

    Trace.currentContext should be (None)

    var called = false
    var childCalled = false
    Trace.trace() {

      Trace.currentContext should not be (None)

      val parent = Trace.currentContext
      Trace.trace() {
        Trace.currentContext should not be (Some(parent))

        Trace.currentContext.get.traceId should be (parent.get.traceId)
        Trace.currentContext.get.spanId should not be (None)
        Trace.currentContext.get.spanId should not be (parent.get.spanId)
        Trace.currentContext.get.parentSpanId should be (parent.get.spanId)
        childCalled = true
      }

      Trace.currentContext should be (parent)
      called = true
    }

    Trace.currentContext should be (None)
    called should be (true)
    childCalled should be (true)

  }

  test("Should fail if new trace context isn't a child of parent context") {

    Trace.currentContext should be (None)

    var called = false
    var childCalled = false
    Trace.trace() {

      Trace.currentContext should not be (None)

      val parent = Trace.currentContext
      evaluating {
        val child = TraceContext()
        Trace.trace(Some(child)) {
          childCalled = true
        }
      } should produce [IllegalArgumentException]

      Trace.currentContext should be (parent)
      called = true
    }

    Trace.currentContext should be (None)
    called should be (true)
    childCalled should be (false)
  }

  test("Time should fail outside of a trace context") {

    val mockTracer: Tracer = mock[Tracer]
    val trace = new Trace {
      override def currentTracer = mockTracer
    }

    var called = false
    evaluating {
      trace.time("I'm outside a trace context!") {
        called = true
      }
    } should produce [IllegalStateException]
    called should be (false)

    verifyZeroInteractions(mockTracer)
  }

  test("Time should be recorded") {

    val message: Message = Message("I'm in a context!")
    val curTime: Long = System.currentTimeMillis
    val mockTracer: Tracer = mock[Tracer]
    var context: Option[TraceContext] = None
    var count = 0;

    val trace = new Trace {
      override def currentTime = curTime  // TODO: Advance time at the second call
      override def currentTracer = mockTracer
    }

    var called = false
    trace.trace() {
      context = trace.currentContext
      trace.time(message.content) {
        called = true
      }
    }

    called should be (true)
    verify(mockTracer).record(Record(context.get, curTime, message, Some(0)))
  }

  test("Record should fail outside of a trace context") {

    val mockTracer: Tracer = mock[Tracer]
    val trace = new Trace {
      override def currentTracer = mockTracer
    }

    evaluating {
      trace.record(Message("I'm outside a trace context!"))
    } should produce [IllegalStateException]

    verifyZeroInteractions(mockTracer)
  }

  test("Record should record!") {

    val message: Message = Message("I'm in a context!")
    val curTime: Long = System.currentTimeMillis
    val mockTracer: Tracer = mock[Tracer]
    var context: Option[TraceContext] = None

    val trace = new Trace {
      override def currentTime = curTime
      override def currentTracer = mockTracer
    }
    trace.trace() {
      context = trace.currentContext
      trace.record(message)
    }

    verify(mockTracer).record(Record(context.get, curTime, message))
  }
}
