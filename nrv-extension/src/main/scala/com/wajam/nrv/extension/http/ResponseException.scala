package com.wajam.nrv.extension.http

class ResponseException(val code: Int, val description: String, val headers: Map[String, String], cause: Throwable)
  extends Exception(s"$code description", cause)

class NotModifiedException(description: String = "Not Modified",
                           headers: Map[String, String] = Map.empty,
                           cause: Throwable = null) extends ResponseException(304, description, headers, cause)

class BadRequestException(description: String = "Bad Request",
                        headers: Map[String, String] = Map.empty,
                        cause: Throwable = null) extends ResponseException(400, description, headers, cause)

class NotFoundException(description: String = "Not Found",
                        headers: Map[String, String] = Map.empty,
                        cause: Throwable = null) extends ResponseException(404, description, headers, cause)

class ServiceUnavailableException(description: String = "Service Unavailable",
                        headers: Map[String, String] = Map.empty,
                        cause: Throwable = null) extends ResponseException(503, description, headers, cause)
