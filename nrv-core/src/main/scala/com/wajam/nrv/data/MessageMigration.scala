package com.wajam.nrv.data

class MessageMigration(val map: scala.collection.Map[String, Any]) {

  private val newSuffix = "_New"

  /**
   * Use to write data on both the new format and the new to allow legacy code to not freak out.
   * Phase #1 of migrating a use case.
   *
   */
  implicit def setValue(map: scala.collection.Map[String, Any], key: String, newValue: Seq[String], oldValue: Any) = {
    map += (key, oldValue)
    map += (key + newSuffix, newValue)
  }

  /**
   * Use to mark a usage compatible (already Seq[String]). Phase #1 of migrating a use case.
   *
   */
  implicit def setNoopValue(map: scala.collection.Map[String, Any], key: String, newValue: Seq[String]) = {
    map += (key, newValue)
  }

  /**
   * Use to revert key_new to key. Phase #2 of migrating a use case.
   *
   */
  implicit def setEitherStringValue(map: scala.collection.Map[String, Any], key: String, newValue: Seq[String]) = {
    map += (key, newValue)
    map += (key_new, newValue)
  }


  /**
   * Return both the new and old value. Phase #1 of migrating a use case.
   */
  implicit def getBothValue(map: scala.collection.Map[String, Any], key: String): (Any, Seq[String]) = {
    (map(key), map(key + newSuffix))
  }

  /**
   * Use to revert key_new to key. Phase #2 of migrating a use case.
   *
   */
  implicit def getEitherStringValue(map: scala.collection.Map[String, Any], key: String, newValue: Seq[String]) = {
    map += (key, newValue)
  }

  /**
   * Get the first value of Seq[String] or String transparently
   */
  implicit def getFlatStringValue(map: scala.collection.Map[String, Any], key: String): Option[String] = {

    // Try the new way Seq[String]
    val anyValue = getRealFlatValue(map, key + newSuffix)

    // Failed? Try the old key
    anyValue match {
      case None => getRealFlatValue(map, key)
      case result => result
    }
  }

  /**
   * Get the first value of Seq[String] or String transparently
   */
  private def getRealFlatValue(map: scala.collection.Map[String, Any], key: String): Option[String] = {
    map.getOrElse(key, null) match {
      case None => None
      case values: Seq[_] if values.isEmpty => None
      case values: Seq[_] => Some(values(0).asInstanceOf[String])
      case value: String => Some(value)
      case _ => None
    }
  }
}
