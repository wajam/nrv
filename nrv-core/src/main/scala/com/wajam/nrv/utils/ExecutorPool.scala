package com.wajam.nrv.utils

import scala.actors.Actor
import com.wajam.nrv.Logging
import com.wajam.nrv.service.Action
import com.wajam.nrv.data.Message
import scala.Int
import scala.util.Random

class ExecutorPool(val numExecutor: Int = 100, executorSelector: (Int) => (Int) = ExecutorPool.defaultExecutorSelector) {

  require(numExecutor > 0)

  private val executors = Array.fill(numExecutor) {
    new Executor
  }

  def start {
    for (e <- executors) {
      e.start()
    }
  }

  def execute(next: () => Unit) {
    val executor = executors(executorSelector(numExecutor))
    executor.execute(next)
  }
}

object ExecutorPool {

  private val random = new Random()

  val defaultExecutorSelector = (executorCount: Int) => {
    random.nextInt(executorCount)
  }
}

private class Executor extends Actor with Logging {

  def execute(next: () => Unit) {
    this ! next
  }

  def act() {
    loop {
      react {
        case next: (() => Unit) =>
          try {
            next()
          } catch {
            case e: Exception => error("Got an error in Executor: ", e)
          }
      }
    }
  }
}
