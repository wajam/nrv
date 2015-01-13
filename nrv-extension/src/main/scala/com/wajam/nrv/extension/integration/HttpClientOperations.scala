package com.wajam.nrv.extension.integration

import java.net.{URLEncoder, HttpURLConnection, URL}
import java.io.{InputStreamReader, InputStream}
import com.wajam.nrv.protocol.codec.Codec

/**
 *
 */

trait HttpClientOperations {

  private val DEFAULT_ENCODING = "UTF-8"

  type RequestData
  type ResponseData

  val codec: Codec
  val defaultRequestContentType: String

  protected def port: Int
  val address: String = "localhost"

  def get(path: String, params: Map[String, String] = Map(), headers: Map[String, String] = Map()): (Int, ResponseData) = {
    val url = buildUrl(path, params)
    val connection = url.openConnection().asInstanceOf[HttpURLConnection]
    for ((key, value) <- headers) connection.setRequestProperty(key, value)
    connection.connect()
    val response = if (connection.getResponseCode >= 400) {
      if (connection.getErrorStream != null) {
        val out = readString(connection.getErrorStream)
        connection.getErrorStream.close()
        out
      } else {
        ""
      }
    } else {
      val out = readString(connection.getInputStream)
      connection.getInputStream.close()
      out
    }
    (connection.getResponseCode, codec.decode(response.getBytes(DEFAULT_ENCODING), DEFAULT_ENCODING).asInstanceOf[ResponseData])
  }

  def post(path: String, data: RequestData, params: Map[String, String] = Map(), headers: Map[String, String] = Map()): (Int, ResponseData) = {
    val url = buildUrl(path, params)
    val connection = url.openConnection().asInstanceOf[HttpURLConnection]
    connection.setRequestProperty("Content-Type", defaultRequestContentType + "; charset=" + DEFAULT_ENCODING)
    for ((key, value) <- headers) connection.setRequestProperty(key, value)
    connection.setDoOutput(true)
    val out = connection.getOutputStream
    val encodedData = codec.encode(data, DEFAULT_ENCODING)
    out.write(encodedData)
    out.close()
    connection.connect()
    val response = if (connection.getResponseCode >= 400) {
      val out = if (connection.getErrorStream != null) {
        val out = readString(connection.getErrorStream)
        connection.getErrorStream.close()
        out
      } else ""
      out
    } else {
      val out = readString(connection.getInputStream)
      connection.getInputStream.close()
      out
    }
    (connection.getResponseCode, codec.decode(response.getBytes(DEFAULT_ENCODING), DEFAULT_ENCODING).asInstanceOf[ResponseData])
  }

  def put(path: String, data: RequestData, params: Map[String, String] = Map(), headers: Map[String, String] = Map()): (Int, ResponseData) = {
    val url = buildUrl(path, params)
    val connection = url.openConnection().asInstanceOf[HttpURLConnection]
    connection.setRequestMethod("PUT")
    connection.setRequestProperty("Content-Type", defaultRequestContentType + "; charset=" + DEFAULT_ENCODING)
    for ((key, value) <- headers) connection.setRequestProperty(key, value)
    connection.setDoOutput(true)
    val out = connection.getOutputStream
    val encodedData = codec.encode(data, DEFAULT_ENCODING)
    out.write(encodedData)
    out.close()
    connection.connect()
    val response = if (connection.getResponseCode >= 400) {
      val out = if (connection.getErrorStream != null) {
        val out = readString(connection.getErrorStream)
        connection.getErrorStream.close()
        out
      } else ""
      out
    } else {
      val out = readString(connection.getInputStream)
      connection.getInputStream.close()
      out
    }
    (connection.getResponseCode, codec.decode(response.getBytes(DEFAULT_ENCODING), DEFAULT_ENCODING).asInstanceOf[ResponseData])
  }

  def delete(path: String, params: Map[String, String] = Map(), headers: Map[String, String] = Map()): (Int, ResponseData) = {
    val url = buildUrl(path, params)
    val connection = url.openConnection().asInstanceOf[HttpURLConnection]
    connection.setRequestMethod("DELETE")
    for ((key, value) <- headers) connection.setRequestProperty(key, value)
    connection.connect()
    val response = if (connection.getResponseCode >= 400) {
      val out = readString(connection.getErrorStream)
      connection.getErrorStream.close()
      out
    } else {
      val out = readString(connection.getInputStream)
      connection.getInputStream.close()
      out
    }
    (connection.getResponseCode, codec.decode(response.getBytes(DEFAULT_ENCODING), DEFAULT_ENCODING).asInstanceOf[ResponseData])
  }

  def waitForCondition[T](block: () => T, condition: (T) => Boolean, sleepTimeInMs: Long = 250, timeoutInMs: Long = 10000) {
    val startTime = System.currentTimeMillis()
    while (!condition(block())) {
      if ((System.currentTimeMillis() - startTime) > timeoutInMs) {
        throw new RuntimeException("Timeout waiting for condition.")
      }
      Thread.sleep(sleepTimeInMs)
    }
  }

  private def buildUrl(baseUrl: String, params: Map[String, String]) = {
    val path = new StringBuilder(baseUrl)
    if (!params.isEmpty) {
      path.append(params.map(p => {
        p._1 + "=" + URLEncoder.encode(p._2, "UTF-8")
      }).mkString(if (baseUrl.contains("?")) "&" else "?", "&", ""))
    }
    new URL("http://" + address + ":" + port + path.toString())
  }

  /**
   * [lifted from lift]
   */
  private def readString(is: InputStream) = {
    val in = new InputStreamReader(is, DEFAULT_ENCODING)
    val bos = new StringBuilder
    val ba = new Array[Char](4096)

    def readOnce() {
      val len = in.read(ba)
      if (len > 0) bos.appendAll(ba, 0, len)
      if (len >= 0) readOnce()
    }

    readOnce()

    bos.toString()
  }

}
