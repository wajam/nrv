package com.wajam.nrv.extension.json.integration

import com.wajam.nrv.extension.json.codec.JsonCodec
import net.liftweb.json.JsonAST.JValue
import com.wajam.nrv.extension.integration.HttpClientOperations

/**
 *
 */
trait JsonHttpClientOperations extends HttpClientOperations {

  type RequestData = Any
  type ResponseData = JValue

  override val codec = new JsonCodec
  override val defaultRequestContentType = "application/json"
 }
