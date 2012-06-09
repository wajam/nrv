package com.wajam.nrv.service

import collection.immutable.TreeSet

/**
 * Consistent hashing ring (http://en.wikipedia.org/wiki/Consistent_hashing)
 */
trait Ring[T] {
  var tree = TreeSet[RingNode]()

  def add(token: Long, elem: T) {
    this.tree += new RingNode(token, Some(elem))
  }

  def resolve(token: Long, count: Int): Seq[RingNode] = {
    var result = this.tree.rangeImpl(Some(new RingNode(token, None)), None).toList.slice(0, count)

    if (result.size < count) {
      result ++= this.tree.slice(0, count - result.size).toList
    }

    result
  }

  def resolve(token: Long, count: Int, filter: RingNode => Boolean): Seq[RingNode] = {
    var result = this.tree.rangeImpl(Some(new RingNode(token, None)), None).filter(filter).toList

    if (result.size < count) {
      result ++= this.tree.slice(0, count - result.size).filter(filter).toList
    }

    result
  }

  def delete(token: Long) {
    this.tree -= new RingNode(token, None)
  }

  def copyTo(that: Ring[T]) {
    for (node <- tree) {
      that.add(node.token, node.value.get)
    }
  }

  def size = this.tree.size

  class RingNode(var token: Long, var value: Option[T]) extends Comparable[RingNode] {
    def compareTo(o: RingNode) = (this.token - o.token).asInstanceOf[Int]
  }
}


