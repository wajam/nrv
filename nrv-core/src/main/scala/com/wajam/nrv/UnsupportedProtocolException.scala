package com.wajam.nrv

/**
* Means that the message received is not part of the valid protocol Nrv can use
*/
class UnsupportedProtocolException(message:String) extends Exception(message) {

}
