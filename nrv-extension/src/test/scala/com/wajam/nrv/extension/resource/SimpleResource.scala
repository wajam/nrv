package com.wajam.nrv.extension.resource

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FunSuite
import com.wajam.nrv.service.ActionMethod

/**
 *
 */
@RunWith(classOf[JUnitRunner])
class TestResource extends FunSuite {

  test("should create correct operations") {
    val getFunction = (r: Request) => {}
    val listFunction = (r: Request) => {}
    val createFunction = (r: Request) => {}
    val updateFunction = (r: Request) => {}
    val deleteFunction = (r: Request) => {}
    val r = new SimpleResource("name", "id", None,
      Some(getFunction), Some(listFunction), Some(createFunction), Some(updateFunction), Some(deleteFunction))

    assert(ActionMethod.GET === r.operations.toSeq(0).method)
    assert(ActionMethod.GET === r.operations.toSeq(1).method)
    assert(ActionMethod.POST === r.operations.toSeq(2).method)
    assert(ActionMethod.PUT === r.operations.toSeq(3).method)
    assert(ActionMethod.DELETE === r.operations.toSeq(4).method)

    assert("/name/:id" === r.operations.toSeq(0).path)
    assert("/name" === r.operations.toSeq(1).path)
    assert("/name" === r.operations.toSeq(2).path)
    assert("/name/:id" === r.operations.toSeq(3).path)
    assert("/name/:id" === r.operations.toSeq(4).path)

    assert(getFunction === r.operations.toSeq(0).operation)
    assert(listFunction === r.operations.toSeq(1).operation)
    assert(createFunction === r.operations.toSeq(2).operation)
    assert(updateFunction === r.operations.toSeq(3).operation)
    assert(deleteFunction === r.operations.toSeq(4).operation)

  }

  test("should create correct path with parent resource") {
    val getFunction = (r: Request) => {}
    val listFunction = (r: Request) => {}
    val createFunction = (r: Request) => {}
    val updateFunction = (r: Request) => {}
    val deleteFunction = (r: Request) => {}
    val r = new SimpleResource("name", "id", Some(new SimpleResource("parent", "pid", None)),
      Some(getFunction), Some(listFunction), Some(createFunction), Some(updateFunction), Some(deleteFunction))

    assert("/parent/:pid/name/:id" === r.operations.toSeq(0).path)
    assert("/parent/:pid/name" === r.operations.toSeq(1).path)
    assert("/parent/:pid/name" === r.operations.toSeq(2).path)
    assert("/parent/:pid/name/:id" === r.operations.toSeq(3).path)
    assert("/parent/:pid/name/:id" === r.operations.toSeq(4).path)

  }

}

case class SimpleResource(resourceName: String,
                          idName: String,
                          parent: Option[Resource],
                          get: Option[(Request) => Unit] = None,
                          list: Option[(Request) => Unit] = None,
                          create: Option[(Request) => Unit] = None,
                          update: Option[(Request) => Unit] = None,
                          delete: Option[(Request) => Unit] = None) extends Resource
