package com.wajam.nrv.tracing

import com.yammer.metrics.core.{TimerContext, MetricName}
import java.util.concurrent.TimeUnit
import com.yammer.metrics.scala.{Instrumented, Timer}
import com.wajam.nrv.tracing.Annotation.Message

/**
 * The mixin trait for creating a class which is traced with NRV tracing and instrumented with metrics.
 */
trait Traced extends Instrumented {

  def tracedTimer(name: String, scope: String = null,
                  durationUnit: TimeUnit = TimeUnit.MILLISECONDS, rateUnit: TimeUnit = TimeUnit.SECONDS) = {
    new TracedTimer(metrics.timer(name, scope, durationUnit, rateUnit, metrics.metricsRegistry),
      new MetricName(getClass, name, scope))
  }
}

/**
 * Wrapper for Yammer metrics Timer
 */
class TracedTimer(val timer: Timer, val name: String, val source: Option[String]) {

  def this(timer: Timer, name: MetricName) {
    this(timer, name.getName, Some(name.getGroup + "." + name.getType))
  }

  /**
   * Runs block, recording its duration, and returns the result of block.
   */
  def time[S](block: => S): S = {
    val tracer = Tracer.currentTracer
    timer.time {
      if (tracer.isDefined) {
        tracer.get.time(name, source) {
          block
        }
      } else {
        block
      }
    }
  }

  /**
   * Adds a recorded duration.
   */
  def update(duration: Long, unit: TimeUnit) {
    val tracer = Tracer.currentTracer
    if (tracer.isDefined) {
      tracer.get.record(Message(name, source), Some(unit.toMillis(duration)))
    }
    timer.update(duration, unit)
  }

  def timerContext(): TracedTimerContext = {
    new TracedTimerContext(timer.timerContext(), Tracer.currentTracer map (
      tracer => tracer.getTracingContext(name, source)))
  }

  class TracedTimerContext(timerContext: TimerContext, optTracerContext: Option[TracingContext]) {
    def stop() {
      timerContext.stop()
      for (tracer <- optTracerContext) tracer.stop()
    }
  }

}
