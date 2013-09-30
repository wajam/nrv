package com.wajam.nrv.tracing

import org.scalatest.{BeforeAndAfter, FunSuite}
import org.scalatest.matchers.ShouldMatchers._
import java.util.{TimeZone, Calendar}
import java.net.{InetAddress, InetSocketAddress}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import com.wajam.commons.InetUtils

/**
 *
 */
@RunWith(classOf[JUnitRunner])
class TestTraceRecordFormatter extends FunSuite {

  val expectedTime = "2012-10-01T16:57:27.992-0400"
  val time: Calendar = Calendar.getInstance(TimeZone.getTimeZone("EST"))
  time.clear()
  time.set(2012, 9, 1, 15, 57, 27)
  time.set(Calendar.MILLISECOND, 992)
  val hostname = InetUtils.firstInetAddress.map(_.getHostName).getOrElse("")

  test("ServRecv should be properly formatted") {
    val expected = expectedTime + "\t\tServerRecv\tTID\tSID\tPID\tservice\tprotocol\tMETHOD\t/resource\t" + hostname + "\t\t\t\t\t"

    val context = TraceContext("TID", "SID", Some("PID"))
    val annotation = Annotation.ServerRecv(new RpcName("service", "protocol", "METHOD", "/resource"))
    val record = new Record(context, time.getTimeInMillis, annotation)

    val actual = TraceRecordFormatter.record2TabSeparatedString(record)
    actual should be (expected)
  }

  test("ServRecv without parent should be properly formatted") {
    val expected = expectedTime + "\t\tServerRecv\tTID\tSID\t\tservice\tprotocol\tMETHOD\t/resource\t" + hostname + "\t\t\t\t\t"

    val context = TraceContext("TID", "SID", None)
    val annotation = Annotation.ServerRecv(new RpcName("service", "protocol", "METHOD", "/resource"))
    val record = new Record(context, time.getTimeInMillis, annotation)

    val actual = TraceRecordFormatter.record2TabSeparatedString(record)
    actual should be (expected)
  }

  test("ServRecv with empty rpc name should be properly formatted") {
    val expected = expectedTime + "\t\tServerRecv\tTID\tSID\tPID\t\t\t\t\t" + hostname + "\t\t\t\t\t"

    val context = TraceContext("TID", "SID", Some("PID"))
    val annotation = Annotation.ServerRecv(new RpcName(null, null, null, null))
    val record = new Record(context, time.getTimeInMillis, annotation)

    val actual = TraceRecordFormatter.record2TabSeparatedString(record)
    actual should be (expected)
  }

  test("ClientSend should be properly formatted") {
    val expected = expectedTime + "\t\tClientSend\tTID\tSID\tPID\tservice\tprotocol\tMETHOD\t/resource\t" + hostname + "\t\t\t\t\t"

    val context = TraceContext("TID", "SID", Some("PID"))
    val annotation = Annotation.ClientSend(new RpcName("service", "protocol", "METHOD", "/resource"))
    val record = new Record(context, time.getTimeInMillis, annotation)

    val actual = TraceRecordFormatter.record2TabSeparatedString(record)
    actual should be (expected)
  }

  test("ClientRecv should be properly formatted") {
    val expected = expectedTime + "\t\tClientRecv\tTID\tSID\tPID\t\t\t\t\t" + hostname + "\t\t\t200\t\t"

    val context = TraceContext("TID", "SID", Some("PID"))
    val annotation = Annotation.ClientRecv(Some(200))
    val record = new Record(context, time.getTimeInMillis, annotation)

    val actual = TraceRecordFormatter.record2TabSeparatedString(record)
    actual should be (expected)
  }

  test("ServerSend should be properly formatted") {
    val expected = expectedTime + "\t\tServerSend\tTID\tSID\tPID\t\t\t\t\t" + hostname + "\t\t\t200\t\t"

    val context = TraceContext("TID", "SID", Some("PID"))
    val annotation = Annotation.ServerSend(Some(200))
    val record = new Record(context, time.getTimeInMillis, annotation)

    val actual = TraceRecordFormatter.record2TabSeparatedString(record)
    actual should be (expected)
  }

  test("ServerSend without response code should be properly formatted") {
    val expected = expectedTime + "\t\tServerSend\tTID\tSID\tPID\t\t\t\t\t" + hostname + "\t\t\t\t\t"

    val context = TraceContext("TID", "SID", Some("PID"))
    val annotation = Annotation.ServerSend(None)
    val record = new Record(context, time.getTimeInMillis, annotation)

    val actual = TraceRecordFormatter.record2TabSeparatedString(record)
    actual should be (expected)
  }

  test("Message should be properly formatted") {
    val expected = expectedTime + "\t\tMessage\tTID\tSID\tPID\t\t\t\t\t" + hostname + "\t\t\t\tThis is a message!\t"

    val context = TraceContext("TID", "SID", Some("PID"))
    val annotation = Annotation.Message("This is a message!")
    val record = new Record(context, time.getTimeInMillis, annotation)

    val actual = TraceRecordFormatter.record2TabSeparatedString(record)
    actual should be (expected)
  }

  test("Message with a source should be properly formatted") {
    val expected = expectedTime + "\t\tMessage\tTID\tSID\tPID\t\t\t\t\t" + hostname + "\t\t\t\tThis is a message!\tSource"

    val context = TraceContext("TID", "SID", Some("PID"))
    val annotation = Annotation.Message("This is a message!", Some("Source"))
    val record = new Record(context, time.getTimeInMillis, annotation)

    val actual = TraceRecordFormatter.record2TabSeparatedString(record)
    actual should be (expected)
  }

  test("Message with duration should be properly formatted") {
    val expected = expectedTime + "\t1234\tMessage\tTID\tSID\tPID\t\t\t\t\t" + hostname + "\t\t\t\tThis is a message!\t"

    val context = TraceContext("TID", "SID", Some("PID"))
    val annotation = Annotation.Message("This is a message!")
    val record = new Record(context, time.getTimeInMillis, annotation, Some(1234))

    val actual = TraceRecordFormatter.record2TabSeparatedString(record)
    actual should be (expected)
  }

  test("Message with tab should be properly escaped") {
    val expected = expectedTime + "\t\tMessage\tTID\tSID\tPID\t\t\t\t\t" + hostname + "\t\t\t\tThere was a tab in this message!\t"

    val context = TraceContext("TID", "SID", Some("PID"))
    val annotation = Annotation.Message("There was a tab\tin this message!")
    val record = new Record(context, time.getTimeInMillis, annotation)

    val actual = TraceRecordFormatter.record2TabSeparatedString(record)
    actual should be (expected)
  }

  test("ClientAddress should be properly formatted") {
    val expected = expectedTime + "\t\tClientAddress\tTID\tSID\tPID\t\t\t\t\twww.google.com\t" + """(?:[0-9]{1,3}\.){3}[0-9]{1,3}""" + "\t80\t\t\t"

    val context = TraceContext("TID", "SID", Some("PID"))
    val annotation = Annotation.ClientAddress(new InetSocketAddress(InetAddress.getByName("www.google.com"), 80))
    val record = new Record(context, time.getTimeInMillis, annotation)

    val actual = TraceRecordFormatter.record2TabSeparatedString(record)
    actual should fullyMatch regex expected
  }

  test("ServerAddress should be properly formatted") {
    val expected = expectedTime + "\t\tServerAddress\tTID\tSID\tPID\t\t\t\t\twww.google.com\t" + """(?:[0-9]{1,3}\.){3}[0-9]{1,3}""" + "\t80\t\t\t"

    val context = TraceContext("TID", "SID", Some("PID"))
    val annotation = Annotation.ServerAddress(new InetSocketAddress(InetAddress.getByName("www.google.com"), 80))
    val record = new Record(context, time.getTimeInMillis, annotation)

    val actual = TraceRecordFormatter.record2TabSeparatedString(record)
    actual should fullyMatch regex expected
  }
}
