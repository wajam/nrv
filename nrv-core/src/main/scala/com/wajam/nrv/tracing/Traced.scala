package com.wajam.nrv.tracing

import com.yammer.metrics.core.MetricName
import java.util.concurrent.TimeUnit
import com.yammer.metrics.scala.{Instrumented, Timer}

/**
 * The mixin trait for creating a class which is traced with NRV tracing and instrumented with metrics.
 */
trait Traced {
  def tracedTimer(name: String, instrumented: Instrumented, scope: String = null,
                  durationUnit: TimeUnit = TimeUnit.MILLISECONDS, rateUnit: TimeUnit = TimeUnit.SECONDS) = {
    new TracedTimer(new MetricName(instrumented.getClass, name, scope),
      instrumented.metrics.timer(name, scope, durationUnit, rateUnit, instrumented.metrics.metricsRegistry))
  }
}

class TracedTimer(name: MetricName, timer: Timer) {

  def time[S](tracer: Option[Tracer])(block: => S) {
    timer.time {
      if (tracer.isDefined) {
        tracer.get.time(name.toString) {
          block
        }
      } else {
        block
      }
    }
  }

}
