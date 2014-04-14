package com.wajam.nrv.consistency

import com.wajam.nrv.utils.timestamp.{Timestamp, TimestampGenerator}
import com.wajam.nrv.utils.TimestampIdGenerator
import com.wajam.commons.SynchronizedIdGenerator

class DummyTimestampGenerator extends TimestampGenerator {
  val idGenerator = new TimestampIdGenerator with SynchronizedIdGenerator[Long]

  def fetchTimestamps(name: String, cb: (Seq[Timestamp], Option[Exception]) => Unit, nb: Int, token: Long) = {
    cb(0.to(nb).map(_ => Timestamp(idGenerator.nextId)), None)
  }

  def responseTimeout = 100L
}
