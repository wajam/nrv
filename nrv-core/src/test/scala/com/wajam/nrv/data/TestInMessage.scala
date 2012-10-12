package com.wajam.nrv.data

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FunSuite

/**
 * 
 */

@RunWith(classOf[JUnitRunner])
class TestInMessage extends FunSuite {

  test("reply with error") {
    val error = new Exception
    var callbackCalled = false
    val inMsg = new InMessage()

    inMsg.replyCallback = m => {
      callbackCalled = true
      assert(error === m.error.get)
    }

    inMsg.replyWithError(error)

    assert(callbackCalled)
  }

}
