package com.wajam.nrv.service

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite}

/**
 * Test class for ActionPathMatcher
 */

@RunWith(classOf[JUnitRunner])
class TestActionPathMatcher extends FunSuite with BeforeAndAfter {

  var matcher: ActionPathMatcher = _

  before {
    matcher = new ActionPathMatcher
  }

  test("should match simple path to action") {
    val usersPath = "/users/"
    val usersAction = new Action(usersPath, (_) => {})
    matcher.registerAction(usersAction)
    assert(usersAction === matcher.matchPath(usersPath, ActionMethod.GET).get)
  }

  test("should match simple path to action1") {
    val usersPath = "/users/"
    val usersAction = new Action(usersPath, (_) => {})
    matcher.registerAction(usersAction)
    assert(usersAction === matcher.matchPath("users", ActionMethod.GET).get)
  }

  test("should match simple path to action with method") {
    val usersPath = "/users/"
    val usersAction = new Action(usersPath, (_) => {}, ActionMethod.POST)
    matcher.registerAction(usersAction)
    assert(usersAction === matcher.matchPath(usersPath, ActionMethod.POST).get)
  }

  test("should match simple path to action with not conventional method") {
    val usersPath = "/users/"
    val usersAction = new Action(usersPath, (_) => {}, "othermethod")
    matcher.registerAction(usersAction)
    assert(usersAction === matcher.matchPath(usersPath, "othermethod").get)
  }

  test("should match simple path to action with not conventional method (different case)") {
    val usersPath = "/users/"
    val usersAction = new Action(usersPath, (_) => {}, "othermethod")
    matcher.registerAction(usersAction)
    assert(usersAction === matcher.matchPath(usersPath, "OtherMethod").get)
  }

  test("should not match action with wrong method") {
    val usersPath = "/users/"
    val usersAction = new Action(usersPath, (_) => {}, ActionMethod.POST)
    matcher.registerAction(usersAction)
    assert(None === matcher.matchPath(usersPath, ActionMethod.GET))
  }

  test("should match empty path to action") {
    val rootAction = new Action("", (_) => {})
    matcher.registerAction(rootAction)
    assert(rootAction === matcher.matchPath("/", ActionMethod.GET).get)
    assert(rootAction === matcher.matchPath("", ActionMethod.GET).get)
  }

  test("should match empty path to action with method") {
    val rootAction = new Action("", (_) => {}, ActionMethod.POST)
    matcher.registerAction(rootAction)
    assert(None === matcher.matchPath("/", ActionMethod.GET))
    assert(rootAction === matcher.matchPath("", ActionMethod.POST).get)
  }

  test("should match complete path") {
    val usersPath = "/users/"
    val usersAction = new Action(usersPath, (_) => {})
    val usersFolloweesPath = "/users/followees/"
    val usersFolloweesAction = new Action(usersFolloweesPath, (_) => {})
    matcher.registerAction(usersAction)
    matcher.registerAction(usersFolloweesAction)

    assert(usersAction === matcher.matchPath(usersPath, ActionMethod.GET).get)
    assert(usersFolloweesAction === matcher.matchPath(usersFolloweesPath, ActionMethod.GET).get)
    assert(None === matcher.matchPath("/users/followees/profiles", ActionMethod.GET))
  }

  test("should not match invalid path") {
    val usersPath = "/users/"
    val usersAction = new Action(usersPath, (_) => {})
    matcher.registerAction(usersAction)

    assert(None === matcher.matchPath("/users/followees", ActionMethod.GET))
  }

  test("should match path with variables") {
    val usersPath = "/users/:user_id"
    val usersAction = new Action(usersPath, (_) => {})
    matcher.registerAction(usersAction)

    assert(usersAction === matcher.matchPath("/users/12345", ActionMethod.GET).get)
  }

  test("should match path with variables only if another path is more strict") {
    val usersPath = "/users/:user_id"
    val usersFolloweesPath = "/users/followees"
    val usersAction = new Action(usersPath, (_) => {})
    val usersFolloweesAction = new Action(usersFolloweesPath, (_) => {})
    matcher.registerAction(usersAction)
    matcher.registerAction(usersFolloweesAction)

    assert(usersFolloweesAction === matcher.matchPath(usersFolloweesPath, ActionMethod.GET).get)
  }

  test("should override with the last registered action") {
    val usersPath = "/users/"
    val usersActionOld = new Action(usersPath, (_) => {})
    val usersActionNew = new Action(usersPath, (_) => {})
    matcher.registerAction(usersActionOld)
    matcher.registerAction(usersActionNew)

    assert(usersActionNew === matcher.matchPath(usersPath, ActionMethod.GET).get)
  }

  test("should allow to register more specific paths") {
    val usersFolloweesPath = "/users/:id/followees"
    val usersFolloweesMePath = "/users/me/followees"
    val usersActionWithId = new Action(usersFolloweesPath, (_) => {})
    val usersActionMe = new Action(usersFolloweesMePath, (_) => {})
    matcher.registerAction(usersActionWithId)
    matcher.registerAction(usersActionMe)

    assert(usersActionMe === matcher.matchPath(usersFolloweesMePath, ActionMethod.GET).get)
    assert(usersActionWithId === matcher.matchPath("/users/12345/followees", ActionMethod.GET).get)
  }

  test("should allow to override path with variables") {
    val usersFolloweesPath = "/users/:id/followees"
    val usersFolloweesNamePath = "/users/:name/followees"
    val usersActionWithId = new Action(usersFolloweesPath, (_) => {})
    val usersActionWithName = new Action(usersFolloweesNamePath, (_) => {})
    matcher.registerAction(usersActionWithId)
    matcher.registerAction(usersActionWithName)

    assert(usersActionWithName === matcher.matchPath("/users/12345/followees", ActionMethod.GET).get)
  }

  test("should allow many path after a variable") {
    val usersFolloweesPath = "/users/:id/followees"
    val usersProfilesPath = "/users/:id/profiles"
    val usersFolloweesAction = new Action(usersFolloweesPath, (_) => {})
    val usersProfilesAction = new Action(usersProfilesPath, (_) => {})
    matcher.registerAction(usersFolloweesAction)
    matcher.registerAction(usersProfilesAction)

    assert(usersFolloweesAction === matcher.matchPath("/users/12345/followees", ActionMethod.GET).get)
    assert(usersProfilesAction === matcher.matchPath("/users/12345/profiles", ActionMethod.GET).get)
  }
}
