package com.wajam.nrv.utils

/**
 Custom equality as recommended by "Programming in scala, Ch.30". =>
 "Pitfall #3: Defining equals in terms of mutable field"
*/
trait ContentEquals {
  def equalsContent(obj: Any): Boolean
}
