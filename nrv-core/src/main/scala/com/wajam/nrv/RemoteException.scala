package com.wajam.nrv

/**
 * Exception that occurred on a remote node
 */
class RemoteException(message: String, original: Throwable) extends Exception(message, original) {

}
