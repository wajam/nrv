package com.wajam.nrv.service

import com.wajam.nrv.InvalidParameter

/**
 * Path used in NRV to represent an action and part of the ActionPath
 */
class ActionPath(var path: String) {
  path = path.trim
  if (path == "")
    path = "/"

  val pathFragments = path.split("/")

  lazy val pathParams:Seq[String] = this.pathFragments.filter(_.startsWith(":"))

  def matchesPath(path: String): (Boolean, scala.collection.Map[String, String]) = {
    val thatPathFragments = path.split("/")

    if (thatPathFragments.size != pathFragments.size)
      return (false, null)

    var dataMap = Map[String, String]()
    for (i <- 0 to pathFragments.size - 1) {
      if (pathFragments(i) startsWith ":") {
        dataMap += (pathFragments(i).substring(1) -> thatPathFragments(i))

      } else if (pathFragments(i) != thatPathFragments(i)) {
        return (false, null)
      }
    }

    (true, dataMap)
  }

  def buildPath(data: scala.collection.Map[String, Any] = Map()):String = {
    var retPath:String = "/"
    for (fragment <- pathFragments) {
      if (retPath != "/")
        retPath += "/"

      if (fragment startsWith ":") {
        val param = fragment.substring(1)
        if (data.contains(param)) {
          retPath += data(param)
        } else {
          throw new InvalidParameter("Missing parameter '%s' for path %s".format(param, path))
        }
      } else {
        retPath += fragment
      }
    }

    retPath
  }

  override def toString = path
}

object ActionPath {
  implicit def string2path(value:String) = new ActionPath(value)
  implicit def path2string(value:ActionPath) = value.toString
}

