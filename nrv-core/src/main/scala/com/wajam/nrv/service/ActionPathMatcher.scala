package com.wajam.nrv.service

import collection.mutable
import annotation.tailrec

/**
 * Optimized path matcher.
 *
 * It builds a tree from the registered actions path and traverse the tree on resolution time.
 */

class ActionPathMatcher {
  val root = new PathMatchingNode()

  def registerAction(action: Action) {
    insertActionInTree(root, action.path.pathFragments.toList, action)
  }

  def matchPath(path: ActionPath, method: ActionMethod): Option[Action] = {
    findActionInTree(root, path.pathFragments.toList, method)
  }

  @tailrec
  private def insertActionInTree(currentNode: PathMatchingNode, subPath: List[String], action: Action) {
    subPath match {
      case Nil => {
        // end of the path, register the action on this node
        currentNode.registerAction(action.method, action)
      }
      case fragment :: restOfThePath => {
        val next = if (fragment.startsWith(":")) {
          currentNode.defaultNextNode match {
            case None => {
              val nextNode = new PathMatchingNode()
              currentNode.setDefaultNextNode(nextNode)
              nextNode
            }
            case Some(nextNode) => nextNode
          }
        } else {
          currentNode.nextNode(fragment) match {
            case None => {
              val nextNode = new PathMatchingNode()
              currentNode.addNextNode(fragment, nextNode)
              nextNode
            }
            case Some(nextNode) => nextNode
          }
        }
        insertActionInTree(next, restOfThePath, action)
      }
    }
  }

  @tailrec
  private def findActionInTree(currentNode: PathMatchingNode, subPath: List[String], method: ActionMethod): Option[Action] = {
    subPath match {
      case Nil => currentNode.matchingActionFor(method)
      case fragment :: restOfThePath => {
        currentNode.nextNodeWithDefault(fragment) match {
          case None => None
          case Some(next) => findActionInTree(next, restOfThePath, method)
        }
      }
    }
  }
}

class PathMatchingNode() {
  private val actionMap = new mutable.HashMap[ActionMethod, Action]()
  private val next = new mutable.HashMap[String, PathMatchingNode]()
  private var defaultNext: Option[PathMatchingNode] = None

  def registerAction(method: ActionMethod, action: Action) {
    actionMap.put(method, action)
  }

  def matchingActionFor(method: ActionMethod): Option[Action] = {
    actionMap.get(method) match {
      case None => actionMap.get(ActionMethod.ANY)
      case Some(action) => Some(action)
    }
  }

  def addNextNode(fragment: String, node: PathMatchingNode) {
    next.put(fragment, node)
  }

  def defaultNextNode = defaultNext

  def setDefaultNextNode(node: PathMatchingNode) {
    defaultNext = Some(node)
  }

  def nextNode(forFragment: String): Option[PathMatchingNode] = {
    next.get(forFragment)
  }

  def nextNodeWithDefault(forFragment: String): Option[PathMatchingNode] = {
    next.get(forFragment) match {
      case None => defaultNext
      case Some(node) => Some(node)
    }
  }
}