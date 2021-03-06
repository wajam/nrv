package com.wajam.nrv.extension.json.codec

import com.wajam.nrv.protocol.codec.Codec
import net.liftweb.json._

/**
 * Json codec used by HttpProtocol
 */

class JsonCodec extends Codec {

  def encode(data: Any, context: Any): Array[Byte] = {
    import JsonCodec._

    val contentEncoding = context.asInstanceOf[String]
    data match {
      case s: String => s.getBytes(contentEncoding)
      case l: Seq[_] => JsonRender.render(toJValue(l)).getBytes(contentEncoding)
      case m: Map[_, _] => JsonRender.render(toJValue(m)).getBytes(contentEncoding)
      case v: JValue => JsonRender.render(v).getBytes(contentEncoding)
      case _ => throw new RuntimeException("Invalid type, can not render json for " + data.getClass)
    }
  }

  def decode(data: Array[Byte], context: Any) = {
    val contentEncoding = context.asInstanceOf[String]
    parse(new String(data, contentEncoding))
  }
}

object JsonCodec {
  def toJValue(value: Any): JValue = {
    value match {
      case v: JValue => v
      case s: String => JString(s)
      case l: Long => JInt(l)
      case bi: BigInt => JInt(bi)
      case i: Int => JInt(i)
      case d: Double => JDouble(d)
      case b: Boolean => JBool(b)
      case tuple: Tuple2[_, _] => toJValue(Map(tuple))
      case seq: Seq[_] => JArray(seq.map(toJValue(_: Any)).toList)
      case map: Map[_, _] => JObject(map.map(e => JField(e._1.toString, toJValue(e._2))).toList)
      case _ => throw new RuntimeException("Invalid type, can not render json for " + value.getClass)
    }
  }
}

/**
 * Json rendering methods
 * Lifted from Lift, then optimized
 */
object JsonRender {
  // https://github.com/lift/lift/blob/master/framework/lift-base/lift-json/src/main/scala/net/liftweb/json/JsonAST.scala#L356
  def render(json: JValue): String = innerRender(new StringBuilder(), json).toString()

  private def innerRender(sb: StringBuilder, json: JValue): StringBuilder = json match {
    case JBool(true) => sb.append("true")
    case JBool(false) => sb.append("false")
    case JDouble(n) => sb.append(n.toString)
    case JInt(n) => sb.append(n.toString())
    case JNull | JString(null) => sb.append("null")
    case JNothing => sb
    case JString(s) => quote(sb.append("\""), s).append("\"")
    case JArray(arr) => {
      sb.append('[')
      arr.foreach {
        case JNothing =>
        case elem => innerRender(sb, elem).append(',')
      }
      appendEnumClosingChar(']', sb)
    }
    case JField(n, v) => {
      sb.append('"').append(n).append('"').append(':')
      innerRender(sb, v)
    }
    case JObject(obj) => {
      sb.append('{')
      obj.foreach {
        case JField(_, JNothing) =>
        case field => innerRender(sb, field).append(',')
      }
      appendEnumClosingChar('}', sb)
    }
  }

  private def appendEnumClosingChar(char: Char, sb: StringBuilder): StringBuilder = {
    if (sb.charAt(sb.length - 1) == ',') {
      sb.setCharAt(sb.length - 1, char)
      sb
    } else {
      sb.append(char)
    }
  }

  // https://github.com/lift/lift/blob/master/framework/lift-base/lift-json/src/main/scala/net/liftweb/json/JsonAST.scala#L397
  private def quote(sb: StringBuilder, s: String): StringBuilder = {
    var i = 0
    val length = s.length
    while (i < length) {
      s.charAt(i) match {
        case '"' => sb.append("\\\"")
        case '\\' => sb.append("\\\\")
        case '\b' => sb.append("\\b")
        case '\f' => sb.append("\\f")
        case '\n' => sb.append("\\n")
        case '\r' => sb.append("\\r")
        case '\t' => sb.append("\\t")
        case c if ((c >= '\u0000' && c <= '\u001f')) => sb.append("\\u%04x".format(c: Int))
        case c => sb.append(c)
      }
      i += 1
    }
    sb
  }
}
