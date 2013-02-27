package com.wajam.nrv.microbenchmarks

import com.google.caliper.{Param, SimpleBenchmark}
import net.liftweb.json.JsonAST._
import com.wajam.nrv.extension.json.codec.JsonRender
import JsonRender.{render => nrvRender}
import net.liftweb.json.Printer

class JsonBenchmark extends SimpleBenchmark {

  @Param(Array("10", "100", "1000"))
  val length: Int = 0

  var values: JArray = JArray(Nil)

  val possibleValues = Map(
    10 -> JArray((for (i <- 1 to 10) yield getRandomFollowee(i)).toList),
    100 -> JArray((for (i <- 1 to 100) yield getRandomFollowee(i)).toList),
    1000 -> JArray((for (i <- 1 to 1000) yield getRandomFollowee(i)).toList)
  )

  private def getRandomString(size: Int): String = {
    (for (_ <- 1 to size) yield util.Random.nextPrintableChar()).mkString
  }

  private def getRandomFollowee(i: Int): JObject = {
    JObject(JField("id", JString(i.toString)) ::
      JField("name", JString(getRandomString(10))) ::
      JField("network", JString("MySpace")) ::
      JField("network_id", JString(getRandomString(10 * length))) ::
      JField("picture", JString(getRandomString(10 * length))) ::
      JField("aggregated", JBool(util.Random.nextBoolean())) ::
      JField("groups", JArray(JObject(JField("a", JString("a")) :: Nil) :: JObject(JField("a", JString("b")) :: Nil) :: Nil)) ::
      Nil)
  }

  override def setUp() {
    values = possibleValues(length)
  }

  def timeLiftRender(reps: Int) {
    for (_ <- (1 to reps)) Printer.compact(render(values))
  }

  def timeNrvRender(reps: Int) {
    for (_ <- (1 to reps)) nrvRender(values)
  }
}
