package com.wajam.nrv.utils

/**
 * Trait that renders an object observable by allowing event listening and triggering.
 */
trait Observable {
  private type Observer = (Event) => Unit
  private var observers = List[Observer]()
  private var parents = List[Observable]()

  def addParentObserver(parent: Observable) {
    assert(parent != this)
    this.parents :+= parent
  }

  def addObserver(cb: Observer) {
    this.observers :+= cb
  }

  protected def notifyObservers(event: Event) {
    observers.foreach(obs => obs(event))
    parents.foreach(parent => parent.notifyObservers(event))
  }
}

class Event

class VotableEvent extends Event {
  var yeaVotes = 0
  var noVotes = 0

  def vote(pass: Boolean) = pass match {
    case true => yeaVotes += 1
    case false => noVotes += 1
  }

}



