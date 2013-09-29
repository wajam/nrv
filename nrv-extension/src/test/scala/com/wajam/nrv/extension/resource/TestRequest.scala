package com.wajam.nrv.extension.resource

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FunSuite
import com.wajam.nrv.data.{MList, InMessage}
import org.scalatest.matchers.ShouldMatchers
import com.wajam.nrv.extension.resource.Request._
import com.wajam.nrv.InvalidParameter

/**
 * Test class for Request
 */
@RunWith(classOf[JUnitRunner])
class TestRequest extends FunSuite with ShouldMatchers {

  test("test parameter extraction to a String") {
    val message = new InMessage()
    message.parameters("ks") = "string"
    message.parameters("ki") = 1
    message.parameters("kl") = 1l
    message.parameters("kd") = 1.2
    message.parameters("kb") = true
    message.parameters("klist") = MList(Seq("a", "b", "c"))

    message.param[String]("ks") should be(Some("string"))
    message.param[String]("ki") should be(Some("1"))
    message.param[String]("kl") should be(Some("1"))
    message.param[String]("kd") should be(Some("1.2"))
    message.param[String]("kb") should be(Some("true"))
    message.param[String]("klist") should be(Some("a"))
  }

  test("test parameter extraction to a Long") {
    val message = new InMessage()
    message.parameters("ks") = "1"
    message.parameters("ki") = 1
    message.parameters("kl") = 1l
    message.parameters("kd") = 1.2
    message.parameters("kb") = true
    message.parameters("klist") = MList(Seq(1, 2, 3))

    message.param[Long]("ks") should be(Some(1))
    message.param[Long]("ki") should be(Some(1))
    message.param[Long]("kl") should be(Some(1))

    intercept[InvalidParameter] {
      message.param[Long]("kd")
    }
    intercept[InvalidParameter] {
      message.param[Long]("kb")
    }

    intercept[InvalidParameter] {
      message.param[Long]("klist")
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

    message.param[Int]("ks") should be(Some(1))
    message.param[Int]("ki") should be(Some(1))

    intercept[InvalidParameter] {
      message.param[Int]("kl")
    }

    intercept[InvalidParameter] {
      message.param[Int]("kd")
    }
    intercept[InvalidParameter] {
      message.param[Int]("kb")
    }

    intercept[InvalidParameter] {
      message.param[Int]("klist")
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

    message.param[Double]("ks") should be(Some(1.0))
    message.param[Double]("ki") should be(Some(1.0))
    message.param[Double]("kl") should be(Some(1.0))
    message.param[Double]("kd") should be(Some(1.2))

    intercept[InvalidParameter] {
      message.param[Double]("kb")
    }

    intercept[InvalidParameter] {
      message.param[Double]("klist")
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

    message.param[Boolean]("ks") should be(Some(true))
    message.param[Boolean]("kb") should be(Some(true))

    intercept[InvalidParameter] {
      message.param[Boolean]("ki")
    }

    intercept[InvalidParameter] {
      message.param[Boolean]("kl")
    }

    intercept[InvalidParameter] {
      message.param[Boolean]("kd")
    }

    intercept[InvalidParameter] {
      message.param[Boolean]("klist")
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

    message.param[List[String]]("ks") should be(Some(List("a")))
    message.param[List[String]]("ki") should be(Some(List("1")))
    message.param[List[String]]("kl") should be(Some(List("1")))
    message.param[List[String]]("kd") should be(Some(List("1.2")))
    message.param[List[String]]("kb") should be(Some(List("true")))
    message.param[List[String]]("klist") should be(Some(List("1", "2", "3")))

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

    message.param[Boolean]("t1") should be(Some(true))
    message.param[Boolean]("t2") should be(Some(true))
    message.param[Boolean]("t3") should be(Some(true))
    message.param[Boolean]("t4") should be(Some(true))
    message.param[Boolean]("f1") should be(Some(false))
    message.param[Boolean]("f2") should be(Some(false))
    message.param[Boolean]("f3") should be(Some(false))

  }

  test("test parameter extraction with default") {
    val message = new InMessage()

    message.param("ks", "default") should be("default")
  }

  test("test checked parameter extraction") {
    val message = new InMessage()

    intercept[InvalidParameter] {
      message.checkedParam[Long]("key")
    }
  }

}
