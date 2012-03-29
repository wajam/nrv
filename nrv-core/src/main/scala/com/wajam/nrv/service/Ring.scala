package com.wajam.nrv.service

import collection.immutable.TreeSet

/**
 * Consistent hashing ring (http://en.wikipedia.org/wiki/Consistent_hashing)
 */
trait Ring[T] {
  var tree = TreeSet[Node]()

  def add(token: Long, elem: T) {
    this.tree += new Node(token, Some(elem))
  }

  def resolve(token: Long, count: Int): List[Node] = {
    var result = this.tree.rangeImpl(Some(new Node(token, None)), None).toList.slice(0, count)

    if (result.size < count) {
      result ++= this.tree.slice(0, count - result.size).toList
    }

    result
  }

  def delete(token: Long) {
    this.tree -= new Node(token, None)
  }

  def size = this.tree.size

  class Node(var token: Long, var value: Option[T]) extends Comparable[Node] {
    def compareTo(o: Node) = (this.token - o.token).asInstanceOf[Int]
  }

}
