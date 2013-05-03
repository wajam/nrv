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

    @tailrec
    def incrementAndUpdateUntilEndTime(value: Long, step: Long, endTime: Long): Long = {
      if (System.currentTimeMillis() < endTime) {
        val incrementedValue = value + step
        atom.update(Some(Timestamp(incrementedValue)))
        incrementAndUpdateUntilEndTime(incrementedValue, step, endTime)
      } else {
        value
      }
    }

    // Launch a few tasks that try to increment the atomic value in parallel with different increment steps.
    val endTime = System.currentTimeMillis() + 200
    val incrementors = Seq(Future(incrementAndUpdateUntilEndTime(2, 3, endTime)),
      Future(incrementAndUpdateUntilEndTime(1, 4, endTime)))

    // The watcher task poll the atomic value until all incrementors are completed. The watcher verify that the value
    // always increment and never goes backward.
    val watcher = Future {
      @tailrec
      def verifyEqualsOrGreaterUntilIncrementorsCompleted(lastValue: Option[Timestamp]) {
        if (!incrementors.forall(_.isCompleted)) {
          val newValue = atom.get
          if (newValue.get < lastValue.get) {
            throw new Exception("%s < %s".format(newValue.get, lastValue.get))
          } else {
            verifyEqualsOrGreaterUntilIncrementorsCompleted(newValue)
          }
        }
      }

      verifyEqualsOrGreaterUntilIncrementorsCompleted(atom.get)
      Timestamp(incrementors.map(Future.blocking(_)).max)
    }

    val value = Future.blocking(watcher)
    value should be > initValue
    Some(value) should be (atom.get)
  }
}
