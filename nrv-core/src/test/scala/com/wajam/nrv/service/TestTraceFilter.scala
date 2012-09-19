package com.wajam.nrv.service

import org.scalatest.{BeforeAndAfter, FunSuite}
import org.scalatest.mock.MockitoSugar

/**
 *
 */
class TestTraceFilter extends FunSuite with BeforeAndAfter with MockitoSugar {

  ignore("Should record incomming request without trace context (brand new context)") {

  }

  ignore("Should record incomming request with current trace context (new child context inherited from current)") {

  }

  ignore("Should record incomming response with matching out message") {

  }

  ignore("Should do 'something' with incomming response without matching out message") {

  }

  ignore("Should record outgoing request with current trace context (new child context inherited from current)") {

  }

  ignore("Should fail on outgoing request without current trace context") {

  }

  ignore("Should record outgoing response with matching out message with current trace context (use current context)") {

  }

  ignore("Should fail on outgoing response without current trace context") {

  }

  ignore("Should record local node address when not 'any' local address (i.e. not 0.0.0.0)") {

  }

  ignore("Should record first local network address when local node address is 0.0.0.0") {

  }
}
