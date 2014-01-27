package com.wajam.nrv.extension.resource

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FunSuite
import com.wajam.nrv.data.{MList, InMessage}
import org.scalatest.{Matchers => ShouldMatchers}
import com.wajam.nrv.extension.resource.ParamsAccessor._
import com.wajam.nrv.InvalidParameter

/**
 * Test class for Request
 */
@RunWith(classOf[JUnitRunner])
class TestParamsAccessor extends FunSuite with ShouldMatchers {

  test("test parameter extraction to a String") {
    val message = new InMessage()
    message.parameters("ks") = "string"
    message.parameters("ki") = 1
    message.parameters("kl") = 1l
    message.parameters("kd") = 1.2
    message.parameters("kb") = true
    message.parameters("klist") = MList(Seq("a", "b", "c"))

    message.optionalParam[String]("ks") should be(Some("string"))
    message.optionalParam[String]("ki") should be(Some("1"))
    message.optionalParam[String]("kl") should be(Some("1"))
    message.optionalParam[String]("kd") should be(Some("1.2"))
    message.optionalParam[String]("kb") should be(Some("true"))
    message.optionalParam[String]("klist") should be(Some("a"))
  }

  test("test parameter extraction to a Long") {
    val message = new InMessage()
    message.parameters("ks") = "1"
    message.parameters("ki") = 1
    message.parameters("kl") = 1l
    message.parameters("kd") = 1.2
    message.parameters("kb") = true
    message.parameters("klist") = MList(Seq(1, 2, 3))

    message.optionalParam[Long]("ks") should be(Some(1))
    message.optionalParam[Long]("ki") should be(Some(1))
    message.optionalParam[Long]("kl") should be(Some(1))

    intercept[InvalidParameter] {
      message.optionalParam[Long]("kd")
    }

    intercept[InvalidParameter] {
      message.optionalParam[Long]("kb")
    }

    intercept[InvalidParameter] {
      message.optionalParam[Long]("klist")
    }
  }

  test("test parameter extraction to a Int") {
    val message = new InMessage()
    message.parameters("ks") = "1"
    message.parameters("ki") = 1
    message.parameters("kl") = 1l
    message.parameters("kd") = 1.2
    message.parameters("kb") = true
    message.parameters("klist") = MList(Seq(1, 2, 3))

    message.optionalParam[Int]("ks") should be(Some(1))
    message.optionalParam[Int]("ki") should be(Some(1))

    intercept[InvalidParameter] {
      message.optionalParam[Int]("kl")
    }

    intercept[InvalidParameter] {
      message.optionalParam[Int]("kd")
    }
    intercept[InvalidParameter] {
      message.optionalParam[Int]("kb")
    }

    intercept[InvalidParameter] {
      message.optionalParam[Int]("klist")
    }
  }

  test("test parameter extraction to a Double") {
    val message = new InMessage()
    message.parameters("ks") = "1"
    message.parameters("ki") = 1
    message.parameters("kl") = 1l
    message.parameters("kd") = 1.2
    message.parameters("kb") = true
    message.parameters("klist") = MList(Seq(1, 2, 3))

    message.optionalParam[Double]("ks") should be(Some(1.0))
    message.optionalParam[Double]("ki") should be(Some(1.0))
    message.optionalParam[Double]("kl") should be(Some(1.0))
    message.optionalParam[Double]("kd") should be(Some(1.2))

    intercept[InvalidParameter] {
      message.optionalParam[Double]("kb")
    }

    intercept[InvalidParameter] {
      message.optionalParam[Double]("klist")
    }
  }

  test("test parameter extraction to a Boolean") {
    val message = new InMessage()
    message.parameters("ks") = "true"
    message.parameters("ki") = 1
    message.parameters("kl") = 1l
    message.parameters("kd") = 1.2
    message.parameters("kb") = true
    message.parameters("klist") = MList(Seq(1, 2, 3))

    message.optionalParam[Boolean]("ks") should be(Some(true))
    message.optionalParam[Boolean]("kb") should be(Some(true))

    intercept[InvalidParameter] {
      message.optionalParam[Boolean]("ki")
    }

    intercept[InvalidParameter] {
      message.optionalParam[Boolean]("kl")
    }

    intercept[InvalidParameter] {
      message.optionalParam[Boolean]("kd")
    }

    intercept[InvalidParameter] {
      message.optionalParam[Boolean]("klist")
    }
  }

  test("test parameter extraction to a List[String]") {
    val message = new InMessage()
    message.parameters("ks") = "a"
    message.parameters("ki") = 1
    message.parameters("kl") = 1l
    message.parameters("kd") = 1.2
    message.parameters("kb") = true
    message.parameters("klist") = MList(Seq("1", "2", "3"))

    message.optionalParam[List[String]]("ks") should be(Some(List("a")))
    message.optionalParam[List[String]]("ki") should be(Some(List("1")))
    message.optionalParam[List[String]]("kl") should be(Some(List("1")))
    message.optionalParam[List[String]]("kd") should be(Some(List("1.2")))
    message.optionalParam[List[String]]("kb") should be(Some(List("true")))
    message.optionalParam[List[String]]("klist") should be(Some(List("1", "2", "3")))

  }

  test("test parameter extraction of Boolean possibilities") {
    val message = new InMessage()

    message.parameters("t1") = "true"
    message.parameters("t2") = "1"
    message.parameters("t3") = "t"
    message.parameters("t4") = "T"
    message.parameters("f1") = "false"
    message.parameters("f2") = "0"
    message.parameters("f3") = "f"

    message.optionalParam[Boolean]("t1") should be(Some(true))
    message.optionalParam[Boolean]("t2") should be(Some(true))
    message.optionalParam[Boolean]("t3") should be(Some(true))
    message.optionalParam[Boolean]("t4") should be(Some(true))
    message.optionalParam[Boolean]("f1") should be(Some(false))
    message.optionalParam[Boolean]("f2") should be(Some(false))
    message.optionalParam[Boolean]("f3") should be(Some(false))

  }

  test("test parameter extraction with default") {
    val message = new InMessage()

    message.param("ks", "default") should be("default")
  }

  test("test checked parameter extraction") {
    val message = new InMessage()

    intercept[InvalidParameter] {
      message.param[Long]("key")
    }
  }

}
