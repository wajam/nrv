package com.wajam.nrv.consistency.persistence

import org.scalatest.{BeforeAndAfter, FunSuite}
import org.scalatest.matchers.ShouldMatchers._
import java.io.File
import java.nio.file.Files
import com.wajam.nrv.utils.timestamp.Timestamp

class TestFileTransactionLog extends TestTransactionBase with BeforeAndAfter {
  var logDir: File = null
  var fileTxLog: FileTransactionLog = null

  before {
    logDir = Files.createTempDirectory("TestFileTransactionLog").toFile
    fileTxLog = new FileTransactionLog("service", 1000, logDir.getAbsolutePath)
  }

  after {
    fileTxLog.close()

    logDir.listFiles().foreach(_.delete())
    logDir.delete()
    logDir = null
  }

  test("should get proper timestamp from log name") {
    fileTxLog.getTimestampFromName("service-0-1.log") should be(Timestamp(1))
    fileTxLog.getTimestampFromName("service-0987654321-1234567890.log") should be(Timestamp(1234567890))
  }

  test("should get proper log name from timestamp") {
    fileTxLog.getNameFromTimestamp(Timestamp(1)) should be("service-0000001000-1.log")
    fileTxLog.getNameFromTimestamp(Timestamp(1234567890)) should be("service-0000001000-1234567890.log")
  }

  test("should get log files sorted") {
    val file0 = new File(logDir, "service-0000001000-0.log")
    val file1 = new File(logDir, "service-0000001000-1.log")
    val file123 = new File(logDir, "service-0000001000-123.log")
    val file999 = new File(logDir, "service-0000001000-999.log")
    val file1234567890 = new File(logDir, "service-0000001000-1234567890.log")
    val other0 = new File(logDir, "service-0000009999-0.log")
    val other123 = new File(logDir, "service-0000009999-123.log")

    val all = List(file123, other123, file1, file999, file0, file1234567890, other0)
    all.foreach(_.createNewFile())

    fileTxLog.getLogFilesFrom(Timestamp(-1)).toList should be(List(file0, file1, file123, file999, file1234567890))
    fileTxLog.getLogFilesFrom(Timestamp(0)).toList should be(List(file0, file1, file123, file999, file1234567890))
    fileTxLog.getLogFilesFrom(Timestamp(1)).toList should be(List(file1, file123, file999, file1234567890))
    fileTxLog.getLogFilesFrom(Timestamp(500)).toList should be(List(file123, file999, file1234567890))
    fileTxLog.getLogFilesFrom(Timestamp(999)).toList should be(List(file999, file1234567890))
    fileTxLog.getLogFilesFrom(Timestamp(1000)).toList should be(List(file999, file1234567890))
    fileTxLog.getLogFilesFrom(Timestamp(9999999999L)).toList should be(List(file1234567890))
  }

  test("should not get any log files when there are no log files") {
    // No file at all
    fileTxLog.getLogFilesFrom(Timestamp(0)).toList should be(List[File]())

    // No files matching this token
    new File(logDir, "service-0000009999-0.log").createNewFile()
    new File(logDir, "service-0000009999-123.log").createNewFile()
    fileTxLog.getLogFilesFrom(Timestamp(0)).toList should be(List[File]())
  }

  test("should append new transaction") {
    Range(0, 10).foreach(i => {
      fileTxLog.append(createTransaction(i, i - 1))
    })

    val files = logDir.list()
    files should be(Array("service-0000001000-0.log"))
  }

  test("should read transaction") {
    fileTxLog.append(createTransaction(0))
    fileTxLog.commit()

    val it = fileTxLog.read(Timestamp(0))
    it.hasNext should be(true)
    assertEquals(createTransaction(0), it.next())
    it.hasNext should  be(false)
    it.close()
  }

  ignore("should append to non empty log directory") {
    fail("Not implemented yet!")
  }

  ignore("should fail when trying to append out of order transactions") {
    // Append a transaction
    // Append a transaction with previous timestamp not matching
    //  Expect an exception
    //  Verify log file size the same as after first append
    // Append a transaction
    //  Verify read only returns first and 3rd append
    fail("Not implemented yet!")
  }

  ignore("should not corrupt transaction log when append fail due to transaction event persistence error") {
    // Append a normal transaction
    // Append a mocked transaction with write that fails (exception)
    //  Verify log file size the same as after first append
    // Append a normal transaction again
    //  Verify read only returns first and 3rd append
    fail("Not implemented yet!")
  }

  ignore("append from a new instance when an empty log file with the same name exist") {
    fail("Not implemented yet!")
  }

  test("should roll log files") {
    Range(0, 10).foreach(i => {
      fileTxLog.append(createTransaction(i, i - 1))
      if ((i + 1) % 4 == 0) {
        fileTxLog.rollLog()
      }
    })

    val files = logDir.list().sorted
    files should be(Array("service-0000001000-0.log", "service-0000001000-4.log", "service-0000001000-8.log"))
  }

  test("multiple roll log calls between append should not fail") {
    fileTxLog.append(createTransaction(0, -1))
    fileTxLog.rollLog()
    fileTxLog.rollLog()
    fileTxLog.rollLog()
    fileTxLog.append(createTransaction(1, 0))

    val files = logDir.list().sorted
    files should be(Array("service-0000001000-0.log", "service-0000001000-1.log"))
  }

  ignore("should automatically roll log file at a given size threshold") {
    fail("Not implemented yet!")
  }

  test("should read transactions from specified timestamp") {
    Range(0, 4).foreach(i => {
      fileTxLog.append(createTransaction(i, i - 1))
    })
    fileTxLog.rollLog()
    Range(4, 8).foreach(i => {
      fileTxLog.append(createTransaction(i, i - 1))
    })
    fileTxLog.rollLog()
    Range(8, 10).foreach(i => {
      fileTxLog.append(createTransaction(i, i - 1))
    })
    fileTxLog.commit()

    // Validate
    val it = fileTxLog.read(Timestamp(5))
    Range(5, 10).foreach(i => {
      it.hasNext should be(true)
      assertEquals(createTransaction(i, i - 1), it.next())
    })
    it.hasNext should  be(false)
    it.close()
  }

  ignore("read should skip empty log file") {
    // Zero size file
    // File with file header written but no transaction
    fail("Not implemented yet!")
  }

  ignore("read corrupted transaction event") {
    // Should stop reading at that transaction i.e. like no more transactions
    fail("Not implemented yet!")
  }

  ignore("read corrupted transaction file header") {
    fail("Not implemented yet!")
  }

  test("should get the last logged timestamp") {
    fileTxLog.append(createTransaction(0))
    fileTxLog.rollLog()
    fileTxLog.append(createTransaction(3, 0))
    fileTxLog.append(createTransaction(4, 3))
    fileTxLog.rollLog()
    fileTxLog.append(createTransaction(999, 4))
    fileTxLog.append(createTransaction(9999, 999))
    fileTxLog.commit()

    fileTxLog.getLastLoggedTimestamp should be(Some(Timestamp(9999)))

    val otherTxLog = new FileTransactionLog("service", 1000, logDir.getAbsolutePath)
    otherTxLog.getLastLoggedTimestamp should be(Some(Timestamp(9999)))
  }

  test("the last logged timestamp should be None when there are no log files") {
    fileTxLog.getLastLoggedTimestamp should be(None)
  }

  ignore("truncate should delete all transactions from specified timestamp") {
    // Remove tx from timestamp to end of file
    // Delete all following files
  }

  ignore("commit") {
    fail("Not implemented yet!")
  }

  ignore("close") {
    fail("Not implemented yet!")
  }
}
