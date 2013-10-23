package com.wajam.nrv

/**
 * Exception throw in messages routing
 */
case class RouteException(msg: String) extends Exception(msg)
