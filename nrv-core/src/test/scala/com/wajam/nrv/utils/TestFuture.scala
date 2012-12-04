package com.wajam.nrv.utils

import org.scalatest.FunSuite
import com.wajam.nrv.TimeoutException

class TestFuture extends FunSuite {

  test("future method runs a block on a seperate thread and complete a future when done") {
    val f = Future {
      Thread.sleep(10)
      "done"
    }

    assert(!f.isCompleted)
    Thread.sleep(100)
    assert(f.isCompleted)

    assert(Future.blocking(f, 1) == "done")
  }

  test("future should complete when promise is completed") {
    val p = Promise[String]

    assert(p.future.value.isEmpty)
    assert(!p.future.isCompleted)

    var onSuccessValue: Option[String] = None
    p.future.onSuccess {
      case value => onSuccessValue = Some(value)
    }
    assert(onSuccessValue.isEmpty)

    p.success("test")

    assert(onSuccessValue.isDefined)
    assert(onSuccessValue.get == "test")

    assert(p.future.value.isDefined)
    assert(p.future.value.get.right.get == "test")
    p.future.onSuccess {
      case value => assert(value == "test")
    }
  }

  test("future should fail when promise is failed") {
    val p = Promise[String]
    val ex = new Exception("test exception")

    var onFailureValue: Option[Throwable] = None
    p.future.onFailure {
      case fail => onFailureValue = Some(fail)
    }
    assert(onFailureValue.isEmpty)

    p.failure(ex)

    assert(onFailureValue.isDefined)
    assert(onFailureValue.get == ex)

    assert(p.future.isCompleted)
    p.future.onFailure {
      case fail => assert(fail == ex)
    }
  }

  test("future can be waited for by using blocking") {
    val p = Promise[String]

    intercept[TimeoutException] {
      val v = Future.blocking(p.future, 10)
    }

    var got: Option[String] = None
    new Thread(new Runnable {
      def run() {
        got = Some(Future.blocking(p.future, 10))
      }
    }).start()

    p.success("test")

    Thread.sleep(100)

    assert(got.get == "test")

    val v = Future.blocking(p.future, 10)
    assert(v == "test")
  }

  test("future should only complete once") {
    val p = Promise[String]
    var count = 0

    p.future.onSuccess {
      case _ => count += 1
    }

    p.success("test")

    intercept[IllegalStateException] {
      p.success("test")
    }

    assert(count == 1)
  }

  test("blocking on a failed future should throw an exception") {
    val p = Promise[String]

    var threadCaught: Option[Throwable] = None
    new Thread(new Runnable {
      def run() {
        try {
          Future.blocking(p.future, 10)
        } catch {
          case e: Exception => threadCaught = Some(e)
        }
      }
    }).start()

    val ex = new Exception("test")
    p.failure(ex)

    Thread.sleep(100)

    assert(threadCaught.get == ex)

    try {
      Future.blocking(p.future, 10)
      fail("Should have caught exception")
    } catch {
      case e: Exception => assert(e.getMessage == "test", e.getMessage)
    }
  }

  test("andThen should call chained future after success") {
    val p1 = Promise[Boolean]
    val p2 = Promise[Boolean]

    var gotFirst: Boolean = false
    p1.future.onSuccess {
      case value =>
        gotFirst = true

    } andThen {
      case value =>
        if (!gotFirst)
          p2.failure(new Exception("Didn't get girst before second"))
        else
          p2.success(true)
    }
    p1.success(true)

    Future.blocking(p2.future)
  }

  test("either should call a future or the other") {
    var p1 = Promise[Boolean]
    var p2 = Promise[Boolean]
    var c = 0
    var p3 = p1.future either p2.future
    p3 onComplete {
      case _ => c += 1
    }
    p1.success(true)
    assert(c == 1)


    p1 = Promise[Boolean]
    p2 = Promise[Boolean]
    c = 0
    p3 = p1.future either p2.future
    p3 onComplete {
      case _ => c += 1
    }
    p2.success(true)
    assert(c == 1)
  }

}
