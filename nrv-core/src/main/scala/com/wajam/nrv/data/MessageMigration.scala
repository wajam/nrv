package com.wajam.nrv.data

class MessageMutableMapMigration(val varMap: scala.collection.mutable.Map[String, Any]) extends MessageMapMigration(varMap) {

  /**
   * Use to write data on both the new format and the new to allow legacy code to not freak out.
   * Phase #1 of migrating a use case.
   *
   */
  def setValue(key: String, newValue: Seq[String], oldValue: Any) = {
    varMap += (key -> oldValue)
    varMap += (key + newSuffix -> newValue)
  }

  /**
   * Use to mark a usage compatible (already Seq[String]). Phase #1 of migrating a use case.
   *
   */
  def setNoopValue(key: String, newValue: Seq[String]) = {
    varMap += (key -> newValue)
  }

  /**
   * Use to revert key_new to key. Phase #2 of migrating a use case.
   *
   */
  def setEitherStringValue(key: String, newValue: Seq[String]) = {
    varMap += (key -> newValue)
    varMap += (key + newSuffix -> newValue)
  }
}

class MessageMapMigration(val map: scala.collection.Map[String, Any]) {

  val newSuffix = "_New"

  /**
   * Return both the new and old value. Phase #1 of migrating a use case.
   */
  def getBothValue(key: String): (Any, Seq[String]) = {
    (map(key), map(key + newSuffix).asInstanceOf[Seq[String]])
  }

  /**
   *  Get old value directly or use transformation function to get value.
   *  Phase #1 of migrating a use case.
   */
  def getValueOldOrFromString(key: String, transFct: Seq[String] => Any): Any = {
    if (map.contains(key + newSuffix))
      transFct(map(key + newSuffix).asInstanceOf[Seq[String]])
    else
      map.get(key)
  }

  /**
   * Check either the old or new value is contained in the map.
   *
   */
  def containsEither(key: String): Boolean = {
    map.contains(key) || map.contains(key + newSuffix)
  }

  /**
   * Use to revert key_new to key.
   * Phase #2 of migrating a use case.
   *
   */
  def getEitherStringValue(key: String, newValue: Seq[String]) = {
    map.getOrElse(key, map.get(key + newSuffix))
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

  implicit def mutableMap2MigrationMap(map: scala.collection.mutable.Map[String, Any]) = new MessageMutableMapMigration(map)
  implicit def map2MigrationMap(map: scala.collection.Map[String, Any]) = new MessageMapMigration(map)
}
