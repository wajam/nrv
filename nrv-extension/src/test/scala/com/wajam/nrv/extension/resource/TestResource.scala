package com.wajam.nrv.extension.resource

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FunSuite
import com.wajam.nrv.service.{ActionPath, Service, ActionMethod}
import com.wajam.nrv.data.InMessage

/**
 * Test class for Resource
 */
@RunWith(classOf[JUnitRunner])
class TestResource extends FunSuite {

  test("should have correct path for GET operation") {
    val testService = new Service("test")

    val resource = new Resource("test", "id") with Get {
      protected def get: (Request) => Unit = ???
    }

    resource.registerTo(testService)

    assert(testService.actions(0).path.toString === "/test/:id")
    assert(testService.actions(0).method === ActionMethod.GET)
  }

  test("should have correct path for LIST operation") {
    val testService = new Service("test")

    val resource = new Resource("test", "id") with List {
      protected def list: (Request) => Unit = ???
    }

    resource.registerTo(testService)

    assert(testService.actions(0).path.toString === "/test")
    assert(testService.actions(0).method === ActionMethod.GET)
  }

  test("should have correct path for CREATE operation") {
    val testService = new Service("test")

    val resource = new Resource("test", "id") with Create {
      protected def create: (Request) => Unit = ???
    }

    resource.registerTo(testService)

    assert(testService.actions(0).path.toString === "/test")
    assert(testService.actions(0).method === ActionMethod.POST)
  }

  test("should have correct path for UPDATE operation") {
    val testService = new Service("test")

    val resource = new Resource("test", "id") with Update {
      protected def update: (Request) => Unit = ???
    }

    resource.registerTo(testService)

    assert(testService.actions(0).path.toString === "/test/:id")
    assert(testService.actions(0).method === ActionMethod.PUT)
  }

  test("should have correct path for DELETE operation") {
    val testService = new Service("test")

    val resource = new Resource("test", "id") with Delete {
      protected def delete: (Request) => Unit = ???
    }

    resource.registerTo(testService)

    assert(testService.actions(0).path.toString === "/test/:id")
    assert(testService.actions(0).method === ActionMethod.DELETE)
  }

  test("should register all actions to a service") {
    val testService = new Service("test")

    val resource = new Resource("test", "id") with Get with List with Create with Update with Delete {
      protected def delete: (Request) => Unit = ???
      protected def update: (Request) => Unit = ???
      protected def get: (Request) => Unit = ???
      protected def list: (Request) => Unit = ???
      protected def create: (Request) => Unit = ???
    }

    resource.registerTo(testService)

    assert(testService.actions.size === 5)
  }

  test("should register all actions of multiple resources to a service") {
    val testService = new Service("test")

    val resource1 = new Resource("test1", "id") with Get with List with Create with Update with Delete {
      protected def delete: (Request) => Unit = ???
      protected def update: (Request) => Unit = ???
      protected def get: (Request) => Unit = ???
      protected def list: (Request) => Unit = ???
      protected def create: (Request) => Unit = ???
    }

    val resource2 = new Resource("test2", "id") with Get with List with Create with Update with Delete {
      protected def delete: (Request) => Unit = ???
      protected def update: (Request) => Unit = ???
      protected def get: (Request) => Unit = ???
      protected def list: (Request) => Unit = ???
      protected def create: (Request) => Unit = ???
    }

    resource1.registerTo(testService)
    resource2.registerTo(testService)

    assert(testService.actions.size === 10)
  }

  test("should allow retrieving a resource action from a service") {
    val testService = new Service("test")

    val resource = new Resource("test", "id") with Get with List with Create with Update with Delete {
      protected def delete: (Request) => Unit = ???
      protected def update: (Request) => Unit = ???
      protected def get: (Request) => Unit = ???
      protected def list: (Request) => Unit = ???
      protected def create: (Request) => Unit = ???
    }

    resource.registerTo(testService)

    assert(resource.getAction(testService).isDefined)
    assert(resource.listAction(testService).isDefined)
    assert(resource.createAction(testService).isDefined)
    assert(resource.updateAction(testService).isDefined)
    assert(resource.deleteAction(testService).isDefined)
  }

}

