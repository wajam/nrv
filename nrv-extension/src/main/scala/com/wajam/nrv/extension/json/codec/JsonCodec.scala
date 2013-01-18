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
      case l: Seq[Any] => compact(render(toJValue(l))).getBytes(contentEncoding)
      case m: Map[String, Any] => compact(render(toJValue(m))).getBytes(contentEncoding)
      case v: JValue => compact(render(v)).getBytes(contentEncoding)
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
