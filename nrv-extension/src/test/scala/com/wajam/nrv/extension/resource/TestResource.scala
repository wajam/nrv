package com.wajam.nrv.extension.resource

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FunSuite
import com.wajam.nrv.service.{Service, ActionMethod}

/**
 * Test class for Resource
 */
@RunWith(classOf[JUnitRunner])
class TestResource extends FunSuite {

  val NoOp = (req: Request) => {}

  test("should have correct path for GET operation") {
    val testService = new Service("test")

    val resource = new Resource("test", "id") with Get {
      protected def get: (Request) => Unit = NoOp
    }

    resource.registerTo(testService)

    assert(testService.actions(0).path.toString === "/test/:id")
    assert(testService.actions(0).method === ActionMethod.GET)
  }

  test("should have correct path for LIST operation") {
    val testService = new Service("test")

    val resource = new Resource("test", "id") with Index {
      protected def index: (Request) => Unit = NoOp
    }

    resource.registerTo(testService)

    assert(testService.actions(0).path.toString === "/test")
    assert(testService.actions(0).method === ActionMethod.GET)
  }

  test("should have correct path for CREATE operation") {
    val testService = new Service("test")

    val resource = new Resource("test", "id") with Create {
      protected def create: (Request) => Unit = NoOp
    }

    resource.registerTo(testService)

    assert(testService.actions(0).path.toString === "/test")
    assert(testService.actions(0).method === ActionMethod.POST)
  }

  test("should have correct path for UPDATE operation") {
    val testService = new Service("test")

    val resource = new Resource("test", "id") with Update {
      protected def update: (Request) => Unit = NoOp
    }

    resource.registerTo(testService)

    assert(testService.actions(0).path.toString === "/test/:id")
    assert(testService.actions(0).method === ActionMethod.PUT)
  }

  test("should have correct path for DELETE operation") {
    val testService = new Service("test")

    val resource = new Resource("test", "id") with Delete {
      protected def delete: (Request) => Unit = NoOp
    }

    resource.registerTo(testService)

    assert(testService.actions(0).path.toString === "/test/:id")
    assert(testService.actions(0).method === ActionMethod.DELETE)
  }

  test("should register all actions to a service") {
    val testService = new Service("test")

    val resource = new Resource("test", "id") with Get with Index with Create with Update with Delete {
      protected def delete: (Request) => Unit = NoOp
      protected def update: (Request) => Unit = NoOp
      protected def get: (Request) => Unit = NoOp
      protected def index: (Request) => Unit = NoOp
      protected def create: (Request) => Unit = NoOp
    }

    resource.registerTo(testService)

    assert(testService.actions.size === 5)
  }

  test("should register all actions of multiple resources to a service") {
    val testService = new Service("test")

    val resource1 = new Resource("test1", "id") with Get with Index with Create with Update with Delete {
      protected def delete: (Request) => Unit = NoOp
      protected def update: (Request) => Unit = NoOp
      protected def get: (Request) => Unit = NoOp
      protected def index: (Request) => Unit = NoOp
      protected def create: (Request) => Unit = NoOp
    }

    val resource2 = new Resource("test2", "id") with Get with Index with Create with Update with Delete {
      protected def delete: (Request) => Unit = NoOp
      protected def update: (Request) => Unit = NoOp
      protected def get: (Request) => Unit = NoOp
      protected def index: (Request) => Unit = NoOp
      protected def create: (Request) => Unit = NoOp
    }

    resource1.registerTo(testService)
    resource2.registerTo(testService)

    assert(testService.actions.size === 10)
  }

  test("should allow retrieving a resource action from a service") {
    val testService = new Service("test")

    val resource = new Resource("test", "id") with Get with Index with Create with Update with Delete {
      protected def delete: (Request) => Unit = NoOp
      protected def update: (Request) => Unit = NoOp
      protected def get: (Request) => Unit = NoOp
      protected def index: (Request) => Unit = NoOp
      protected def create: (Request) => Unit = NoOp
    }

    resource.registerTo(testService)

    assert(resource.get(testService).isDefined)
    assert(resource.index(testService).isDefined)
    assert(resource.create(testService).isDefined)
    assert(resource.update(testService).isDefined)
    assert(resource.delete(testService).isDefined)
  }

}

