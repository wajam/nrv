package com.wajam.nrv

/**
 * Exception throw in messages routing
 */
class RouteException(msg: String, val function: Int) extends Exception(msg) {

}
