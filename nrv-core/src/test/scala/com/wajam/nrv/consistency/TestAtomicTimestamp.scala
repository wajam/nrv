package com.wajam.nrv.consistency

import org.scalatest.FunSuite
import AtomicTimestamp._
import org.scalatest.matchers.ShouldMatchers._
import com.wajam.nrv.utils.timestamp.Timestamp
import com.wajam.nrv.utils.{Promise, Future}
import scala.annotation.tailrec

class TestAtomicTimestamp extends FunSuite {
  test("update None over None") {
    val atom = new AtomicTimestamp(updateIfGreater, None)
    atom.get should be (None)
    atom.update(None)
    atom.get should be (None)
  }

  test("update with value over None should update the atomic value") {
    val expectedValue = Some(Timestamp(1L))

    val atom = new AtomicTimestamp(updateIfGreater, None)
    atom.get should be (None)
    atom.update(expectedValue)
    atom.get should be (expectedValue)
  }

  test("update None over value should ignore None") {
    val initValue = Some(Timestamp(1L))

    val atom = new AtomicTimestamp(updateIfGreater, initValue)
    atom.get should be (initValue)
    atom.update(None)
    atom.get should be (initValue)
  }

  test("update with greater value should update the atomic value") {
    val initValue = Some(Timestamp(1L))
    val expectedValue = Some(Timestamp(2L))

    initValue.get should be < expectedValue.get
    val atom = new AtomicTimestamp(updateIfGreater, initValue)
    atom.get should be (initValue)
    atom.update(expectedValue)
    atom.get should be (expectedValue)
  }

  test("update with lesser value should NOT update the atomic value") {
    val initValue = Some(Timestamp(1L))
    val newValue = Some(Timestamp(-1L))

    initValue.get should be > newValue.get
    val atom = new AtomicTimestamp(updateIfGreater, initValue)
    atom.get should be (initValue)
    atom.update(newValue)
    atom.get should be (initValue)
  }

  test("concurent update") {
    val initValue = Timestamp(0L)
    val atom = new AtomicTimestamp(updateIfGreater, Some(initValue))

    val done = Promise[Boolean]

    @tailrec
    def updateAndIncrementUntilDone(value: Long, step: Long) {
      if (!done.future.isCompleted) {
        atom.update(Some(Timestamp(value)))
        updateAndIncrementUntilDone(value + step, step)
      }
    }

    Future {
      updateAndIncrementUntilDone(2, 3)
    }

    Future {
      updateAndIncrementUntilDone(1, 4)
    }

    // Verify that atomic value never goes backward
    Future {
      var lastValue = atom.get
      while(!done.future.isCompleted) {
        val newValue = atom.get
        if (newValue.get < lastValue.get) {
          done.failure(new Exception("%s < %s".format(newValue.get, lastValue.get)))
        } else {
          lastValue = newValue
        }
      }
    }

    Thread.sleep(200)
    atom.get.get should be > initValue
    done.success(true)
    done.future.value should be (Some(Right(true)))
  }
}
