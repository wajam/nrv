package com.wajam.nrv.service

import collection.immutable.TreeSet

/**
 * Consistent hashing ring (http://en.wikipedia.org/wiki/Consistent_hashing)
 */
trait Ring[T] {
  private var tree = TreeSet[RingNode]()

  def size = this.tree.size

  def add(token: Long, element: T) {
    this.tree += new RingNode(token, element)
  }

  def nodes: Iterable[RingNode] = this.tree

  def find(token: Long): Option[RingNode] = this.resolve(token, 1) match {
    case Seq() => None
    case Seq(node: RingNode) => if (node.token == token) Some(node) else None
  }

  def resolve(token: Long, count: Int): Seq[RingNode] = this.resolve(token, count, _ => true)

  def resolve(token: Long, count: Int, filter: (RingNode => Boolean)): Seq[RingNode] = {
    val boundNode = Some(new EmptyRingNode(token))
    var result = this.tree.rangeImpl(boundNode, None).filter(filter).toList

    if (result.size < count) {
      result ++= this.tree.rangeImpl(None, boundNode).filter(filter).toList
    }

    result.slice(0, count)
  }

  def delete(token: Long) {
    this.tree -= new EmptyRingNode(token)
  }

  def copyTo(that: Ring[T]) {
    for (node <- tree) {
      that.add(node.token, node.element)
    }
  }

  class RingNode(val token: Long, val element: T) extends Comparable[RingNode] {
    def compareTo(o: RingNode) = this.token.compareTo(o.token)

    override def toString: String = "%s@%d".format(element, token)
  }

  private class EmptyRingNode(token: Long) extends RingNode(token, null.asInstanceOf[T])

}


