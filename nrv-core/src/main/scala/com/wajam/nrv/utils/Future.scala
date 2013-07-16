package com.wajam.nrv.utils

import com.wajam.nrv.TimeoutException

/*
 * Implementation of the "Future" API as proposed in Scala SIP-14
 * (http://docs.scala-lang.org/sips/pending/futures-promises.html), since it's not available in any stable
 * version of scala yet. A stable implementation of the API for future Scala version can be found on
 * https://github.com/phaller/scala/blob/execution-context/src/library/scala/concurrent/ from which traits of NRV's
 * implementation are partly taken from.
 */
trait Future[T] extends Awaitable[T] {
  def onSuccess[U](pf: PartialFunction[T, U]): this.type = onComplete {
    case Left(t) => // do nothing
    case Right(v) => if (pf isDefinedAt v) pf(v)
    else {
      /*do nothing*/
    }
  }

  def onFailure[U](callback: PartialFunction[Throwable, U]): this.type = onComplete {
    case Left(t) => if (callback.isDefinedAt(t)) callback(t)
    else {
      /*do nothing*/
    }
    case Right(v) => // do nothing
  }

  def onComplete[U](func: Either[Throwable, T] => U): this.type

  def isCompleted: Boolean

  def value: Option[Either[Throwable, T]]

  /* Monadic operations */

  def foreach[U](f: T => U): Unit = onComplete {
    case Right(r) => f(r)
    case Left(_) => // do nothing
  }

  def map[S](f: T => S): Future[S] = {
    val p = Promise[S]

    onComplete {
      case Left(t) => p failure t
      case Right(v) =>
        try
          p success f(v)
        catch {
          case t: Throwable => p failure (t)
        }
    }

    p.future
  }

  def flatMap[S](f: T => Future[S]): Future[S] = {
    val p = Promise[S]

    onComplete {
      case Left(t) => p failure t
      case Right(v) =>
        try {
          f(v) onComplete {
            case Left(t) => p failure t
            case Right(v) => p success v
          }
        } catch {
          case t: Throwable => p failure (t)
        }
    }

    p.future
  }

  def fallbackTo[U >: T](that: Future[U]): Future[U] = {
    val p = Promise[U]
    onComplete {
      case r@Right(_) => p complete r
      case _ => p completeWith that
    }
    p.future
  }


  def andThen[U](pf: PartialFunction[Either[Throwable, T], U]): Future[T] = {
    val p = Promise[T]

    onComplete {
      case r =>
        try if (pf isDefinedAt r) pf(r)
        finally p complete r
    }

    p.future
  }

  def either[U >: T](that: Future[U]): Future[U] = {
    val p = Promise[U]

    val completePromise: PartialFunction[Either[Throwable, U], _] = {
      case Left(t) => p tryFailure t
      case Right(v) => p trySuccess v
    }

    this onComplete completePromise
    that onComplete completePromise

    p.future
  }
}

trait Promise[T] {

  def future: Future[T]

  def complete(result: Either[Throwable, T]): this.type =
    if (tryComplete(result)) this else throw new IllegalStateException("Promise already completed.")

  def complete(value: T, throwable: Option[Throwable]): this.type =
    if (throwable match {
      case Some(ex) => tryComplete(Left(ex))
      case _ => tryComplete(Right(value))
    }) this
    else throw new IllegalStateException("Promise already completed.")

  final def tryCompleteWith(other: Future[T]): this.type = {
    other onComplete {
      this tryComplete _
    }
    this
  }

  final def completeWith(other: Future[T]): this.type = {
    other onComplete {
      this complete _
    }
    this
  }

  def tryComplete(result: Either[Throwable, T]): Boolean

  def success(v: T): this.type = complete(Right(v))

  def trySuccess(value: T): Boolean = tryComplete(Right(value))

  def failure(t: Throwable): this.type = complete(Left(t))

  def tryFailure(t: Throwable): Boolean = tryComplete(Left(t))

}

object Future {

  private var threadInitNumber: Int = 0

  private def nextThreadNum = synchronized {
    threadInitNumber += 1
    threadInitNumber - 1
  }

  def apply[U](f: => U): Future[U] = this.future(f)

  def future[U](f: => U): Future[U] = {
    val p = Promise[U]

    // TODO: run on a thread pool
    new Thread(new Runnable {
      def run() {
        try {
          p.success(f)
        } catch {
          case t: Throwable => p.failure(t)
        }
      }
    }, "Future-" + nextThreadNum).start()

    p.future
  }

  def blocking[F](waited: Awaitable[F], atMost: Long = 0): F = {
    waited.result(atMost)
  }
}

object Promise {
  def apply[T]: Promise[T] = new PromiseImpl[T]
}

class PromiseImpl[T] extends Promise[T] with Future[T] {
  @volatile
  private var prvValue: Option[Either[Throwable, T]] = None

  private var callbacks = List[Either[Throwable, T] => Any]()

  def value: Option[Either[Throwable, T]] = prvValue

  def future: Future[T] = this

  @throws(classOf[TimeoutException])
  def ready(atMost: Long): this.type = {
    if (prvValue.isEmpty) {
      synchronized {
        if (prvValue.isEmpty) {
          val start = System.currentTimeMillis()
          this.wait(atMost)

          if (prvValue.isEmpty)
            throw new TimeoutException("Couldn't get future in time", Some(System.currentTimeMillis() - start))
        }
      }
    }
    this
  }

  @throws(classOf[Exception])
  def result(atMost: Long): T = {
    this.ready(atMost)

    this.value.get match {
      case Left(throwable) => throw throwable
      case Right(value) => value
    }
  }

  def onComplete[U](func: Either[Throwable, T] => U): this.type = {
    synchronized {
      prvValue match {
        case None =>
          callbacks :+= func
        case Some(v) => func(v)
      }
    }
    this
  }

  def isCompleted: Boolean = prvValue.isDefined

  def tryComplete(result: Either[Throwable, T]): Boolean = {
    synchronized {
      prvValue match {
        case None =>
          prvValue = Some(result)
          callbacks.map(try {
            _(result)
          })
          this.notifyAll()

          true
        case Some(_) =>
          false
      }
    }
  }
}

trait Awaitable[T] {
  @throws(classOf[TimeoutException])
  def ready(atMost: Long): this.type

  @throws(classOf[Exception])
  def result(atMost: Long): T
}

