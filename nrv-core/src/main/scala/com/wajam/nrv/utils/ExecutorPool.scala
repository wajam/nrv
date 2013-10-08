package com.wajam.nrv.utils

import scala.actors.Actor
import com.wajam.commons.Logging
import scala.util.Random

class ExecutorPool(val numExecutor: Int) {

  require(numExecutor > 0)

  val executorSelector = ExecutorPool.defaultExecutorSelector

  private val executors = Array.fill(numExecutor) {
    new Executor
  }

  def start {
    for (e <- executors) {
      e.start()
    }
  }

  def execute(computation: () => Unit) {
    val executor = executors(executorSelector(numExecutor))
    executor.execute(computation)
  }
}

object ExecutorPool {

  private val random = new Random()

  val defaultExecutorSelector = (executorCount: Int) => {
    random.nextInt(executorCount)
  }
}

private class Executor extends Actor with Logging {

  def execute(computation: () => Unit) {
    this ! computation
  }

  def act() {
    loop {
      react {
        case computation: Function0[_] =>
          try {
            computation()
          } catch {
            case e: Exception => error("Got an error in Executor: ", e)
          }
      }
    }
  }
}
