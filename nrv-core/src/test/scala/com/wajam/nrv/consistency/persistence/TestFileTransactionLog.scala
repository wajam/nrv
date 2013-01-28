package com.wajam.nrv.consistency.persistence

import org.scalatest.BeforeAndAfter
import org.scalatest.matchers.ShouldMatchers._
import java.io.{DataOutputStream, ByteArrayOutputStream, IOException, File}
import java.nio.file.Files
import com.wajam.nrv.utils.timestamp.Timestamp
import org.mockito.Matchers._
import org.mockito.Mockito._
import util.Random

class TestFileTransactionLog extends TestTransactionBase with BeforeAndAfter {
  var logDir: File = null
  var fileTxLog: FileTransactionLog = null
  var spySerializer: TransactionEventSerializer = null

  before {
    logDir = Files.createTempDirectory("TestFileTransactionLog").toFile
    spySerializer = spy(new TransactionEventSerializer)
    fileTxLog = createFileTransactionLog()
  }

  after {
    spySerializer = null

    fileTxLog.close()
    fileTxLog = null

    logDir.listFiles().foreach(_.delete())
    logDir.delete()
    logDir = null
  }

  def createFileTransactionLog(service: String = "service", token: Long = 1000) = {
    new FileTransactionLog(service, token, logDir.getAbsolutePath, spySerializer)
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
    it.hasNext should be(false)
    it.close()
  }

  test("should append to non empty log directory") {
    val tx1 = createTransaction(74)
    val tx2 = createTransaction(4321, 74)
    val tx3 = createTransaction(9999, 4321)

    fileTxLog.append(tx1)
    fileTxLog.append(tx2)
    fileTxLog.commit()
    fileTxLog.close()

    fileTxLog = createFileTransactionLog()
    fileTxLog.append(tx3)
    fileTxLog.commit()

    val files = logDir.list().sorted
    files should be(Array("service-0000001000-74.log", "service-0000001000-9999.log"))

    val expectedTx = List(tx1, tx2, tx3)
    val actualTx = fileTxLog.read(Timestamp(0)).toList
    actualTx.length should be(expectedTx.length)
    expectedTx.zip(actualTx).foreach(tuple => assertEquals(tuple._1, tuple._2))
  }

  test("should fail when trying to append out of order transactions") {
    val tx1 = createTransaction(100) // First
    val tx2 = createTransaction(101, 99) // Bad previous
    val tx3 = createTransaction(102, 100) // Good previous
    val logFile = new File(logDir, fileTxLog.getNameFromTimestamp(tx1.timestamp))

    fileTxLog.append(tx1)
    fileTxLog.commit()
    val fileLenAfterTx1 = logFile.length()
    fileLenAfterTx1 should be > 0L

    // Append a transaction with previous timestamp not matching
    evaluating {
      fileTxLog.append(tx2)
    } should produce[IOException]
    fileTxLog.commit()
    logFile.length() should be(fileLenAfterTx1)

    fileTxLog.append(tx3)
    fileTxLog.commit()
    logFile.length() should be > fileLenAfterTx1

    val expectedTx = List(tx1, tx3)
    val actualTx = fileTxLog.read(Timestamp(0)).toList
    actualTx.length should be(expectedTx.length)
    expectedTx.zip(actualTx).foreach(tuple => assertEquals(tuple._1, tuple._2))
  }

  test("should fail when trying to append a timestamp in the past") {
    val tx1 = createTransaction(100)      // First
    val tx2 = createTransaction(99, 100)  // Bad timestamp
    val tx3 = createTransaction(101, 100) // Good
    val logFile = new File(logDir, fileTxLog.getNameFromTimestamp(tx1.timestamp))

    fileTxLog.append(tx1)
    fileTxLog.commit()
    val fileLenAfterTx1 = logFile.length()
    fileLenAfterTx1 should be > 0L

    // Append a transaction with proper previous but timestamp before previous
    evaluating {
      fileTxLog.append(tx2)
    } should produce[IOException]
    fileTxLog.commit()
    logFile.length() should be(fileLenAfterTx1)

    fileTxLog.append(tx3)
    fileTxLog.commit()
    logFile.length() should be > fileLenAfterTx1

    val expectedTx = List(tx1, tx3)
    val actualTx = fileTxLog.read(Timestamp(0)).toList
    actualTx.length should be(expectedTx.length)
    expectedTx.zip(actualTx).foreach(tuple => assertEquals(tuple._1, tuple._2))
  }

  test("should not corrupt transaction log when append fail due to transaction event persistence error") {
    val tx1 = createTransaction(100)      // First
    val tx2 = createTransaction(150, 100) // Error
    when(spySerializer.serialize(tx2)).thenThrow(new IOException("Forced error"))
    val tx3 = createTransaction(200, 100) // Ok
    val logFile = new File(logDir, fileTxLog.getNameFromTimestamp(tx1.timestamp))

    // Append successful
    fileTxLog.append(tx1)
    fileTxLog.commit()
    val fileLenAfterTx1 = logFile.length()
    fileLenAfterTx1 should be > 0L

    // Append error on transaction serialization, should not  modify the log file
    evaluating {
      fileTxLog.append(tx2)
    } should produce[IOException]
    fileTxLog.commit()
    logFile.length() should be(fileLenAfterTx1)

    // Append successful again
    fileTxLog.append(tx3)
    fileTxLog.commit()
    logFile.length() should be > fileLenAfterTx1

    val expectedTx = List(tx1, tx3)
    val actualTx = fileTxLog.read(Timestamp(0)).toList
    actualTx.length should be(expectedTx.length)
    expectedTx.zip(actualTx).foreach(tuple => assertEquals(tuple._1, tuple._2))
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
    fileTxLog.append(createTransaction(0))
    fileTxLog.append(createTransaction(1, 0))
    fileTxLog.append(createTransaction(2, 1))
    fileTxLog.append(createTransaction(3, 2))
    fileTxLog.rollLog()
    fileTxLog.append(createTransaction(4, 3))
    fileTxLog.append(createTransaction(5, 4))
    fileTxLog.append(createTransaction(6, 5))
    fileTxLog.append(createTransaction(7, 6))
    fileTxLog.rollLog()
    fileTxLog.append(createTransaction(8, 7))
    fileTxLog.append(createTransaction(9, 8))
    fileTxLog.commit()

    // Validate starting at 5
    val it = fileTxLog.read(Timestamp(5))
    Range(5, 10).foreach(i => {
      it.hasNext should be(true)
      assertEquals(createTransaction(i, i - 1), it.next())
    })
    it.hasNext should be(false)
    it.close()

    // Validate starting from every index
    Range(0, 10).foreach(i => {
      fileTxLog.read(Timestamp(i)).zipWithIndex.foreach(tup => {
        val (actual, j) = tup
        assertEquals(createTransaction(j + i, j + i - 1), actual)
      })
    })
  }

  test("read should skip empty log files") {
    val tx1 = createTransaction(100)        // Good
    val fileTx1 = new File(logDir, fileTxLog.getNameFromTimestamp(tx1.timestamp))

    val tx2 = createTransaction(150, 100)   // Good
    val fileTx2 = new File(logDir, fileTxLog.getNameFromTimestamp(tx2.timestamp))

    val tx3 = createTransaction(200, 150)   // Bad
    when(spySerializer.serialize(tx3)).thenThrow(new IOException("Forced error"))
    val fileTx3 = new File(logDir, fileTxLog.getNameFromTimestamp(tx3.timestamp))

    val tx4 = createTransaction(250, 150)   // Good
    val fileTx4 = new File(logDir, fileTxLog.getNameFromTimestamp(tx4.timestamp))

    // Append first transaction
    fileTxLog.append(tx1)
    fileTxLog.commit()
    fileTx1.length() should be > 0L

    // Create an empty log file
    new File(logDir, fileTxLog.getNameFromTimestamp(Timestamp(125))).createNewFile()

    // This append create a new log file
    fileTxLog.rollLog()
    fileTxLog.append(tx2)
    fileTxLog.commit()
    fileTx2.length() should be > 0L

    // Append again on a new log file but serialization fail, should result on a log containing only file headers
    fileTxLog.rollLog()
    evaluating {
      fileTxLog.append(tx3)
    } should produce[IOException]
    fileTxLog.commit()
    fileTx3.length() should be > 0L
    fileTx3.length() should be < fileTx2.length()

    // Append final transaction in a new log file
    fileTxLog.rollLog()
    fileTxLog.append(tx4)
    fileTxLog.commit()
    fileTx4.length() should be > 0L

    val expectedFiles = Array("service-0000001000-100.log", "service-0000001000-125.log",
      "service-0000001000-150.log", "service-0000001000-200.log", "service-0000001000-250.log")
    val actualFiles = logDir.list().sorted
    actualFiles should be(expectedFiles)

    val expectedTx = List(tx1, tx2, tx4)
    val actualTx = fileTxLog.read(Timestamp(0)).toList
    actualTx.length should be(expectedTx.length)
    expectedTx.zip(actualTx).foreach(tuple => assertEquals(tuple._1, tuple._2))
  }

  test("read corrupted transaction event stop at corrupted tx") {
    val tx1 = createTransaction(0)
    val tx2 = createTransaction(1, 0)
    val tx3 = createTransaction(2, 1)
    val tx4 = createTransaction(3, 2)
    val tx5 = createTransaction(4, 3)

    fileTxLog.append(tx1)
    fileTxLog.append(tx2)
    fileTxLog.append(tx3)
    fileTxLog.append(tx4)
    fileTxLog.rollLog()
    fileTxLog.append(tx5)
    fileTxLog.commit()

    // Fail deserialization of the fourth transaction
    doReturn(tx1).doReturn(tx2).doReturn(tx3).doThrow(new IOException()).doReturn(tx5).when(spySerializer).deserialize(anyObject())

    val expectedTx = List(tx2, tx3) // Start at second transaction
    val actualTx = fileTxLog.read(Timestamp(1)).toList
    actualTx.length should be(expectedTx.length)
    expectedTx.zip(actualTx).foreach(tuple => assertEquals(tuple._1, tuple._2))
  }

  test("read corrupted transaction file header") {
    val tx1 = createTransaction(10)
    val tx2 = createTransaction(20, 10)
    val tx3 = createTransaction(30, 20)

    fileTxLog.append(tx1)
    fileTxLog.append(tx2)
    fileTxLog.rollLog()
    fileTxLog.append(tx3)
    fileTxLog.commit()

    // Create a log file between tx2 and tx3 with random content
    val buff = new Array[Byte](25)
    Random.nextBytes(buff)
    Files.write(new File(logDir, fileTxLog.getNameFromTimestamp(Timestamp(25))).toPath, buff)

    val expectedFiles = Array("service-0000001000-10.log", "service-0000001000-25.log", "service-0000001000-30.log")
    val actualFiles = logDir.list().sorted
    actualFiles should be(expectedFiles)

    val expectedTx = List(tx1, tx2)
    val actualTx = fileTxLog.read(Timestamp(1)).toList
    actualTx.length should be(expectedTx.length)
    expectedTx.zip(actualTx).foreach(tuple => assertEquals(tuple._1, tuple._2))
  }

  test("read transaction file with invalid header values") {

    // Create an empty log file with specified header values
    def createEmptyLogFileWithHeader(timestamp: Long, magic: Long = FileTransactionLog.LogFileMagic,
                                     version: Int = FileTransactionLog.LogFileVersion, service: String = fileTxLog.service) {
      val baos = new ByteArrayOutputStream()
      val dos = new DataOutputStream(baos)

      dos.writeLong(magic)
      dos.writeInt(version)
      dos.writeUTF(service)
      dos.flush()
      baos.toByteArray

      Files.write(new File(logDir, fileTxLog.getNameFromTimestamp(Timestamp(timestamp))).toPath, baos.toByteArray)
    }

    // Ensure we can successfully create a log file with valid headers
    fileTxLog.append(createTransaction(10))
    fileTxLog.commit()
    createEmptyLogFileWithHeader(0)
    fileTxLog.read(Timestamp(0)).length should be(1)

    // Invalid header magic
    createEmptyLogFileWithHeader(0, magic = Random.nextLong())
    fileTxLog.read(Timestamp(0)).length should be(0)

    // Invalid header version
    createEmptyLogFileWithHeader(0, version = 1)
    fileTxLog.read(Timestamp(0)).length should be(0)

    // Invalid header service
    createEmptyLogFileWithHeader(0, service = "dummy")
    fileTxLog.read(Timestamp(0)).length should be(0)

    // Test again with a valid header
    createEmptyLogFileWithHeader(0)
    fileTxLog.read(Timestamp(0)).length should be(1)
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

    // Close and try with a brand new instance
    fileTxLog.close()
    fileTxLog = createFileTransactionLog()
    fileTxLog.getLastLoggedTimestamp should be(Some(Timestamp(9999)))
  }

  test("the last logged timestamp should be None when there are no log files") {
    fileTxLog.getLastLoggedTimestamp should be(None)
  }

  test("should get the last logged timestamp even if last log file is empty") {
    fileTxLog.append(createTransaction(100))
    fileTxLog.commit()

    // Create an empty log file
    new File(logDir, fileTxLog.getNameFromTimestamp(Timestamp(200))).createNewFile()

    val expectedFiles = Array("service-0000001000-100.log", "service-0000001000-200.log")
    val actualFiles = logDir.list().sorted
    actualFiles should be(expectedFiles)

    fileTxLog.getLastLoggedTimestamp should be(Some(Timestamp(100)))

    // Close and try with a brand new instance
    fileTxLog.close()
    fileTxLog = createFileTransactionLog()
    fileTxLog.getLastLoggedTimestamp should be(Some(Timestamp(100)))
  }

  test("should get the last logged timestamp even if last log file contains file header only") {
    val tx1 = createTransaction(100)
    val fileTx1 = new File(logDir, fileTxLog.getNameFromTimestamp(tx1.timestamp))
    val tx2 = createTransaction(200, 100)
    when(spySerializer.serialize(tx2)).thenThrow(new IOException())
    val fileTx2 = new File(logDir, fileTxLog.getNameFromTimestamp(tx2.timestamp))

    fileTxLog.append(tx1)
    fileTxLog.rollLog()

    // Tx2 serialization should fail, resulting to a log file containing only file headers
    fileTxLog.rollLog()
    evaluating {
      fileTxLog.append(tx2)
    } should produce[IOException]
    fileTxLog.commit()
    fileTx2.length() should be > 0L
    fileTx2.length() should be < fileTx1.length()

    val expectedFiles = Array("service-0000001000-100.log", "service-0000001000-200.log")
    val actualFiles = logDir.list().sorted
    actualFiles should be(expectedFiles)

    fileTxLog.getLastLoggedTimestamp should be(Some(Timestamp(100)))

    // Close and try with a brand new instance
    fileTxLog.close()
    fileTxLog = createFileTransactionLog()
    fileTxLog.getLastLoggedTimestamp should be(Some(Timestamp(100)))
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
