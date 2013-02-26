package com.wajam.nrv.extension.json

import org.scalatest.FunSuite
import net.liftweb.json.JsonAST._
import com.wajam.nrv.extension.json.codec.JsonRender
import JsonRender.{render => nrvRender}
import net.liftweb.json.Printer

class TestJsonRenderer extends FunSuite {

  def testValue(value: JValue) {
    assert(Printer.compact(render(value)) === nrvRender(value))
  }

  test("should have the same output than Lift for simple document") {
    val length = 1

    def getRandomFollowee: JObject = {
      JObject(JField("id", JString(util.Random.nextInt(10).toString)) ::
        JField("name", JString((for (_ <- 1 to 10) yield util.Random.nextPrintableChar()).mkString)) ::
        JField("network", JString("MySpace")) ::
        JField("network_id", JString((for (_ <- 1 to 10) yield util.Random.nextPrintableChar()).mkString)) ::
        JField("picture", JString((for (_ <- 1 to 10) yield util.Random.nextPrintableChar()).mkString)) ::
        JField("aggregated", JBool(util.Random.nextBoolean())) ::
        JField("groups", JArray(JObject(JField("a", JString("a")) :: Nil) :: JObject(JField("a", JString("b")) :: Nil) :: Nil)) ::
        Nil)
    }
    testValue(JArray((1 to length map (_ => getRandomFollowee)).toList))
  }

  test("should have the same output than Lift for complex document") {
    val length = 100

    def getRandomFollowee: JObject = {
      JObject(JField("id", JString(util.Random.nextInt(10 * length).toString)) ::
        JField("name", JString(util.Random.nextString(10))) ::
        JField("network", JString("MySpace")) ::
        JField("network_id", JString(util.Random.nextString(10))) ::
        JField("picture", JString(util.Random.nextString(20))) ::
        JField("aggregated", JBool(util.Random.nextBoolean())) ::
        JField("groups", JArray(JObject(JField("a", JString("a")) :: Nil) :: JObject(JField("a", JString("b")) :: Nil) :: Nil)) ::
        Nil)
    }
    testValue(JArray((1 to length map (_ => getRandomFollowee)).toList))
  }

  test("should support empty object") {
    testValue(JObject(Nil))
  }

  test("should support empty array") {
    val value = JObject(List(JField("total_found", JInt(0)),
      JField("count", JInt(0)),
      JField("next_offset", JInt(0)),
      JField("records", JArray(List()))))
    testValue(value)
  }

  test("should have the same output than Lift for escaped strings") {
    testValue(JString("\'\"\\\b\f\n\r\t"))
  }

  test("should handle utf-8 characters well") {
    for (c <- '\u0000' to '\u001f') testValue(JString(c.toString))
    for (c <- '\u0080' to '\u00a0') testValue(JString(c.toString))
    for (c <- '\u2000' to '\u2100') testValue(JString(c.toString))
  }
}
