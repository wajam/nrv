package com.wajam.nrv.data

class MessageMigration(val map: scala.collection.mutable.Map[String, Any]) {

  private val newSuffix = "_New"

  /**
   * Use to write data on both the new format and the new to allow legacy code to not freak out.
   * Phase #1 of migrating a use case.
   *
   */
  def setValue(key: String, newValue: Seq[String], oldValue: Any) = {
    map += (key -> oldValue)
    map += (key + newSuffix -> newValue)
  }

  /**
   * Use to mark a usage compatible (already Seq[String]). Phase #1 of migrating a use case.
   *
   */
  def setNoopValue(key: String, newValue: Seq[String]) = {
    map += (key -> newValue)
  }

  /**
   * Use to revert key_new to key. Phase #2 of migrating a use case.
   *
   */
  def setEitherStringValue(key: String, newValue: Seq[String]) = {
    map += (key -> newValue)
    map += (key + newSuffix -> newValue)
  }


  /**
   * Return both the new and old value. Phase #1 of migrating a use case.
   */
  def getBothValue(key: String): (Any, Seq[String]) = {
    (map(key), map(key + newSuffix).asInstanceOf[Seq[String]])
  }

  /**
   * Use to revert key_new to key. Phase #2 of migrating a use case.
   *
   */
  def getEitherStringValue(key: String, newValue: Seq[String]) = {
    map += (key -> newValue)
  }

  /**
   * Get the first value of Seq[String] or String transparently
   */
  def getFlatStringValue(key: String): Option[String] = {

    // Try the new way Seq[String]
    val anyValue = getRealFlatValue(key + newSuffix)

    // Failed? Try the old key
    anyValue match {
      case None => getRealFlatValue(key)
      case result => result
    }
  }

  /**
   * Get the first value of Seq[String] or String transparently
   */
  def getRealFlatValue(key: String): Option[String] = {
    map.getOrElse(key, null) match {
      case None => None
      case values: Seq[_] if values.isEmpty => None
      case values: Seq[_] => Some(values(0).asInstanceOf[String])
      case value: String => Some(value)
      case _ => None
    }
  }
}

object MessageMigration  {
  implicit def map2MigrationMap(map: scala.collection.mutable.Map[String, Any]) = new MessageMigration(map)
}
