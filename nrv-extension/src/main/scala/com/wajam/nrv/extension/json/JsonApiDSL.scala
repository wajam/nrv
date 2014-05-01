package com.wajam.nrv.extension.json

import com.wajam.nrv.service.{ ActionMethod, Action, ActionPath, Service }
import com.wajam.nrv.data.InMessage
import com.wajam.nrv.InvalidParameter
import net.liftweb.json._
import net.liftweb.json.JsonDSL._
import com.wajam.nrv.data.MString
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.{ Try, Failure, Success }
import com.wajam.tracing.TracingExecutionContext

trait JsonApiDSL extends Service {

  def ec: ExecutionContext

  val RESPONSE_HEADERS = Map(
    "Content-Type" -> "application/json; charset=UTF-8",
    "Access-Control-Allow-Origin" -> "*",
    "Access-Control-Allow-Methods" -> "GET,POST,PUT,DELETE",
    "Access-Control-Allow-Headers" -> "Content-Type")

  implicit val formats = Serialization.formats(NoTypeHints)

  // Used for CORS (https://developer.mozilla.org/en-US/docs/HTTP/Access_control_CORS). Some browser/client relies on that
  // to fetch the allowed origins/methods
  private def registerEmptyOptions(path: ActionPath) = {
    this.registerAction(new Action(path, request => {
      request.replyEmpty(200)
    }, new ActionMethod("OPTIONS")))
  }

  abstract sealed class EndPoint {
    val url: String

    val actionMethod: ActionMethod
  }

  case class GET(url: String) extends EndPoint {
    override val actionMethod: ActionMethod = ActionMethod.GET
  }

  case class POST(url: String) extends EndPoint {
    override val actionMethod: ActionMethod = ActionMethod.POST
  }

  case class PUT(url: String) extends EndPoint {
    override val actionMethod: ActionMethod = ActionMethod.PUT
  }

  case class DELETE(url: String) extends EndPoint {
    override val actionMethod: ActionMethod = ActionMethod.DELETE
  }

  case class ANY(url: String) extends EndPoint {
    override val actionMethod: ActionMethod = ActionMethod.ANY
  }

  implicit class ActionWrapper(endpoint: EndPoint) {
    def returnsJsonIn[T <: AnyRef](f: (InMessage, ExecutionContext) => Future[Option[T]]) = {
      registerEmptyOptions(endpoint.url)
      registerAction(new Action(endpoint.url, i => {
        implicit val tec: ExecutionContext = new TracingExecutionContext(ec)
        val fut = Try(f(i, tec)) match {
          case Success(r) => r
          case Failure(t) => Future.failed(t)
        }
        fut.onComplete {
          case Success(Some(v)) => i.replyJson(v)
          case Success(None) => i.replyEmpty()
          case Failure(t) => i.replyException(t)
        }
      }, endpoint.actionMethod))
    }

    def receivesAndReturnsJsonIn[I <: AnyRef, O <: AnyRef](f: (I, InMessage, ExecutionContext) => Future[Option[O]])(
      implicit mf: scala.reflect.Manifest[I]) = {
      import scala.util.control.Exception._

      val handle = handling(classOf[Exception]) by (t => Future.failed(t))
      returnsJsonIn((req, tec) => {
        handle(f(req.getData[JObject].extract[I], req, tec))
      })
    }

    // Alias methods
    def ->[T <: AnyRef](f: (InMessage, ExecutionContext) => Future[Option[T]]) = returnsJsonIn[T](f)
    def ->>[I <: AnyRef, O <: AnyRef](f: (I, InMessage, ExecutionContext) => Future[Option[O]])(
      implicit mf: scala.reflect.Manifest[I]) = receivesAndReturnsJsonIn[I,O](f)
  }

  implicit class JsonRequest(msg: InMessage) {
    def paramString(param: String): String = msg.parameters.get(param) match {
      case Some(v) => v.asInstanceOf[MString].value
      case None =>
        replyError(s"Parameter $param must be specified", 400)
        throw new InvalidParameter(s"Parameter $param must be specified")
    }

    def paramOptionalString(param: String): Option[String] = msg.parameters.get(param).map(_.asInstanceOf[MString].value)

    def paramBoolean(param: String): Boolean = paramString(param) match {
      case "1" => true
      case "true" => true
      case _ => false
    }

    def paramOptionalBoolean(param: String): Option[Boolean] = paramOptionalString(param).map {
      case "1" => true
      case "true" => true
      case _ => false
    }

    def paramLong(param: String): Long = msg.parameters.get(param) match {
      case Some(v) =>
        try {
          v.asInstanceOf[MString].value.toLong
        } catch {
          case e: Exception =>
            replyError(s"Parameter $param must be numeric", 400)
            throw e
        }
      case None =>
        replyError(s"Parameter $param must be specified", 400)
        throw new InvalidParameter(s"Parameter $param must be specified")
    }

    def paramOptionalLong(param: String): Option[Long] = msg.parameters.get(param).map(_.asInstanceOf[MString].value.toLong)

    def paramInt(param: String): Int = msg.parameters.get(param) match {
      case Some(v) =>
        try {
          v.asInstanceOf[MString].value.toInt
        } catch {
          case e: Exception =>
            replyError(s"Parameter $param must be numeric", 400)
            throw e
        }
      case None =>
        replyError(s"Parameter $param must be specified", 400)
        throw new InvalidParameter(s"Parameter $param must be specified")
    }

    def paramOptionalInt(param: String): Option[Int] = msg.parameters.get(param).map(_.asInstanceOf[MString].value.toInt)

    def replyEmpty(code: Int = 200) {
      msg.reply(Map(), RESPONSE_HEADERS, "", code)
    }

    def replyJobject(jObj: JObject, code: Int = 200) {
      msg.reply(Map(), RESPONSE_HEADERS, jObj, code)
    }

    def replyJson(obj: AnyRef, code: Int = 200) {
      msg.reply(Map(), RESPONSE_HEADERS, Serialization.write(obj), code)
    }

    def replyError(message: String, code: Int = 500) {
      val jObj = ("error" -> code) ~ ("message" -> message)
      msg.reply(Map(), RESPONSE_HEADERS, jObj, code)
    }

    def replyException(t: Throwable) {
      val realThrowable = Option(t.getCause).getOrElse(t)

      val jObj =
        ("error" -> 500) ~
          ("type" -> realThrowable.getClass.toString) ~
          ("message" -> realThrowable.getMessage) ~
          ("stack" -> realThrowable.getStackTraceString)

      msg.reply(Map(), RESPONSE_HEADERS, jObj, 500)
      throw t
    }
  }

}
