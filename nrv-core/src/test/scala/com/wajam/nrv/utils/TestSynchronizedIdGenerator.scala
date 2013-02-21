package com.wajam.nrv.utils

import org.scalatest.FunSuite
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.matchers.ShouldMatchers._

@RunWith(classOf[JUnitRunner])
class TestSynchronizedIdGenerator extends FunSuite {

  class SequenceIdGenerator extends IdGenerator[Int] {
    var lastId = 0

    def nextId = {
      lastId += 1
      lastId
    }
  }

  test("concurent calls should get duplicates without synchronized") {
    val generator = new SequenceIdGenerator
    val iterations = 500

    // Generate ids concurently
    val workers = 0.to(5).map(_ => Future.future({
      for (i <- 1 to iterations) yield generator.nextId
    }))

    val ids = (for (worker <- workers) yield Future.blocking(worker)).flatten.toList
    ids.size should be(workers.size * iterations)
    ids.size should be > ids.distinct.size
  }

  test("concurent calls should not get any duplicates") {
    val generator = new SequenceIdGenerator with SynchronizedIdGenerator[Int]
    val iterations = 500

    // Generate ids concurently
    val workers = 0.to(5).map(_ => Future.future({
      for (i <- 1 to iterations) yield generator.nextId
    }))

    val ids = (for (worker <- workers) yield Future.blocking(worker)).flatten.toList
    ids.size should be(workers.size * iterations)
    ids.size should be(ids.distinct.size)
  }
}
