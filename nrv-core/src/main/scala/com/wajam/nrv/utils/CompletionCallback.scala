package com.wajam.nrv.utils

/**
 * This class...
 *
 * User: felix
 * Date: 12/04/12
 */

trait CompletionCallback {

  def operationComplete(result: Option[Throwable])

}
