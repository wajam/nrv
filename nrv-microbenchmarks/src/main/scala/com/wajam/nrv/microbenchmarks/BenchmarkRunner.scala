package com.wajam.nrv.microbenchmarks

import com.google.caliper.{Runner => CaliperRunner}

object BenchmarkRunner extends App {
  CaliperRunner.main(classOf[JsonBenchmark], args)
}
