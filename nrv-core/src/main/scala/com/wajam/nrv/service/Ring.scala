package com.wajam.nrv.service

import collection.immutable.TreeSet

/**
 * Consistent hashing ring (http://en.wikipedia.org/wiki/Consistent_hashing)
 */
trait Ring[T] {
  private var tree = TreeSet[RingNode]()

  def size = this.tree.size

  def add(token: Long, elem: T) {
    this.tree += new RingNode(token, Some(elem))
  }

  def nodes: Iterable[RingNode] = this.tree

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
      that.add(node.token, node.value)
    }
  }

  class RingNode(var token: Long, optValue: Option[T]) extends Comparable[RingNode] {
    def compareTo(o: RingNode) = (this.token - o.token).asInstanceOf[Int]

    def value = optValue.get

    override def toString: String = "%s@%d".format(value, token)
  }

}


