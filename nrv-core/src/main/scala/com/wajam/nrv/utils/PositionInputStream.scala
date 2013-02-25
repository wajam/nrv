package com.wajam.nrv.utils

import java.io.{FilterInputStream, InputStream}

/**
 * a class that keeps track of the position
 * in the input stream. The position points to offset
 * that has been consumed by the applications. It can
 * wrap buffered input streams to provide the right offset
 * for the application.
 */
class PositionInputStream(in: InputStream) extends FilterInputStream(in) {

  private var _position = 0L

  def position: Long = _position

  override def read: Int = {
    val rc: Int = super.read
    if (rc > -1) {
      _position += 1
    }
    rc
  }

  override def read(b: Array[Byte]): Int = {
    val rc: Int = super.read(b)
    if (rc > 0) {
      _position += rc
    }
    rc
  }

  override def read(b: Array[Byte], off: Int, len: Int): Int = {
    val rc: Int = super.read(b, off, len)
    if (rc > 0) {
      _position += rc
    }
    rc
  }

  override def skip(n: Long): Long = {
    val rc: Long = super.skip(n)
    if (rc > 0) {
      _position += rc
    }
    rc
  }

  override def markSupported: Boolean = {
    false
  }

  override def mark(readLimit: Int) {
    throw new UnsupportedOperationException("mark")
  }

  override def reset() {
    throw new UnsupportedOperationException("reset")
  }
}
