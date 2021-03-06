package com.wajam.nrv.consistency.replication

import com.wajam.nrv.data.{Message, MLong, MString}

object ReplicationAPIParams {
  val Token = "token"
  val Start = "start_ts"
  val End = "end_ts"
  val Timestamp = "timestamp"
  val ConsistentTimestamp = "consistent_ts"
  val SessionId = "session_id"
  val Sequence = "seq"
  val Mode = "mode"
  val Cookie = "cookie"

  def getOptionalParamStringValue(key: String)(implicit message: Message): Option[String] = {
    message.parameters.get(key) match {
      case Some(MString(value)) => Some(value)
      case Some(value) => throw new Exception("'%s' unssuported value type (%s)".format(key, value))
      case None => None
    }
  }

  def getParamStringValue(key: String)(implicit message: Message): String = {
    getOptionalParamStringValue(key) match {
      case Some(value) => value
      case None => throw new Exception("'%s' not found".format(key))
    }
  }

  def getOptionalParamLongValue(key: String)(implicit message: Message): Option[Long] = {
    message.parameters.get(key) match {
      case Some(MLong(value)) => Some(value)
      case Some(MString(value)) => Some(value.toLong)
      case Some(value) => throw new Exception("'%s' unssuported value type (%s)".format(key, value))
      case None => None
    }
  }

  def getParamLongValue(key: String)(implicit message: Message): Long = {
    getOptionalParamLongValue(key) match {
      case Some(value) => value
      case None => throw new Exception("'%s' not found".format(key))
    }
  }

  def getSessionId(implicit message: Message): String = {
    getParamStringValue(SessionId)
  }
}

sealed trait ReplicationMode

object ReplicationMode {
  def apply(value: String): ReplicationMode = value match {
    case Live.toString => Live
    case Bootstrap.toString | "store" => Bootstrap
  }

  object Live extends ReplicationMode {
    override val toString = "live"
  }

  object Bootstrap extends ReplicationMode{
    override val toString = "bootstrap"
  }
}
