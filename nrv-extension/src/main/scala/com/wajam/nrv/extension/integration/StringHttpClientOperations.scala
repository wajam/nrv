package com.wajam.nrv.extension.integration

import com.wajam.nrv.protocol.codec.StringCodec

/**
 *
 */
trait StringHttpClientOperations extends HttpClientOperations {

  type RequestData = String
  type ResponseData = String

  override val codec = new StringCodec
  override val defaultRequestContentType = "text/plain"
}
