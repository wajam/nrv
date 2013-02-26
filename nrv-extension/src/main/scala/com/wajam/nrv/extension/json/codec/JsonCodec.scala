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
      case l: Seq[Any] => JsonRender.render(toJValue(l)).getBytes(contentEncoding)
      case m: Map[String, Any] => JsonRender.render(toJValue(m)).getBytes(contentEncoding)
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
      case i: Int => JInt(i)
      case d: Double => JDouble(d)
      case b: Boolean => JBool(b)
      case seq: Seq[Any] => JArray(seq.map(toJValue(_: Any)).toList)
      case map: Map[String, Any] => JObject(map.map(e => JField(e._1, toJValue(e._2))).toList)
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
    case JNull => sb.append("null")
    case JNothing => sb
    case JString(s) => quote(sb.append("\""), s).append("\"")
    case JArray(arr) => {
      sb.append('[')
      arr.filter(_ != JNothing) foreach (elem => {
        innerRender(sb, elem)
        sb.append(',')
      })
      appendEnumClosingChar(']', sb)
    }
    case JField(n, v) => {
      sb.append('"').append(n).append('"').append(':')
      innerRender(sb, v)
    }
    case JObject(obj) => {
      sb.append('{')
      obj.filter(_.value != JNothing).foreach(field => {
        innerRender(sb, field)
        sb.append(',')
      })
      appendEnumClosingChar('}', sb)
    }
  }

  private def appendEnumClosingChar(char: Char, sb: StringBuilder): StringBuilder = {
    if(sb.charAt(sb.length - 1) == ',') {
      sb.setLength(sb.length - 1)
    }
    sb.append(char)
  }

  // https://github.com/lift/lift/blob/master/framework/lift-base/lift-json/src/main/scala/net/liftweb/json/JsonAST.scala#L397
  private def quote(sb: StringBuilder, s: String): StringBuilder = {
    for (c <- s) sb.append(c match {
      case '"' => "\\\""
      case '\\' => "\\\\"
      case '\b' => "\\b"
      case '\f' => "\\f"
      case '\n' => "\\n"
      case '\r' => "\\r"
      case '\t' => "\\t"
      case _ if ((c >= '\u0000' && c < '\u001f') || (c >= '\u0080' && c < '\u00a0') || (c >= '\u2000' && c < '\u2100')) => "\\u%04x".format(c: Int)
      case _ => c
    })
    sb
  }
}
