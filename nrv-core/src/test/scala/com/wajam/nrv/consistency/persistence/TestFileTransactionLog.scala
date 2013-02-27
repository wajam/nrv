package com.wajam.nrv.consistency.persistence

import org.scalatest.BeforeAndAfter
import org.scalatest.matchers.ShouldMatchers._
import java.io.{DataOutputStream, ByteArrayOutputStream, IOException, File}
import java.nio.file.Files
import com.wajam.nrv.utils.timestamp.Timestamp
import org.mockito.Matchers._
import org.mockito.Mockito._
import util.Random
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import com.wajam.nrv.consistency.persistence.LogRecord.Index
import com.wajam.nrv.consistency.TestTransactionBase

@RunWith(classOf[JUnitRunner])
class TestFileTransactionLog extends TestTransactionBase with BeforeAndAfter {
  var logDir: File = null
  var fileTxLog: FileTransactionLog = null
  var spySerializer: LogRecordSerializer = null

  before {
    logDir = Files.createTempDirectory("TestFileTransactionLog").toFile
    spySerializer = spy(new LogRecordSerializer)
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

  def createFileTransactionLog(service: String = "service", token: Long = 1000,
                               dir: String = logDir.getAbsolutePath, fileRolloverSize: Int = 0) = {
    new FileTransactionLog(service, token, dir, fileRolloverSize = fileRolloverSize, serializer = spySerializer)
  }

  test("should get proper index from log name") {
    fileTxLog.getIndexFromName("service-0-1:.log") should be(Index(1, None))
    fileTxLog.getIndexFromName("service-0987654321-1234567890:555555555.log") should be(
      Index(1234567890, Some(Timestamp(555555555))))
  }

  test("should get proper log name from index") {
    fileTxLog.getNameFromIndex(Index(1, None)) should be("service-0000001000-1:.log")
    fileTxLog.getNameFromIndex(Index(1234567890, Some(Timestamp(555555555)))) should be(
      "service-0000001000-1234567890:555555555.log")
  }

  test("should get log files sorted") {
    val file0 = new File(logDir, "service-0000001000-0:100.log")
    val file1 = new File(logDir, "service-0000001000-1:100.log")
    val file123 = new File(logDir, "service-0000001000-123:200.log")
    val file999 = new File(logDir, "service-0000001000-999:300.log")
    val file1234567890 = new File(logDir, "service-0000001000-1234567890:400.log")
    val other0 = new File(logDir, "service-0000009999-0.log") // Ignored
    val other123 = new File(logDir, "service-0000009999-123.log") // Ignored

    val all = List(file123, other123, file1, file999, file0, file1234567890, other0)
    all.foreach(_.createNewFile())

    // All files
    fileTxLog.getLogFiles.toList should be(List(file0, file1, file123, file999, file1234567890))

    // Id only
    fileTxLog.getLogFiles(Index(0)).toList should be(List(file0, file1, file123, file999, file1234567890))
    fileTxLog.getLogFiles(Index(1)).toList should be(List(file1, file123, file999, file1234567890))
    fileTxLog.getLogFiles(Index(500)).toList should be(List(file123, file999, file1234567890))
    fileTxLog.getLogFiles(Index(999)).toList should be(List(file999, file1234567890))
    fileTxLog.getLogFiles(Index(1000)).toList should be(List(file999, file1234567890))
    fileTxLog.getLogFiles(Index(9999999999L)).toList should be(List(file1234567890))
  }

  test("should guess the log file likely to contain the record for specified timestamp") {
    val file10 = new File(logDir, "service-0000001000-10:100.log")
    val file11 = new File(logDir, "service-0000001000-11:100.log")
    val file123 = new File(logDir, "service-0000001000-123:200.log")
    val file999 = new File(logDir, "service-0000001000-999:300.log")
    val file1234567890 = new File(logDir, "service-0000001000-1234567890:400.log")
    val other0 = new File(logDir, "service-0000009999-0.log") // Ignored
    val other123 = new File(logDir, "service-0000009999-123.log") // Ignored

    val allFiles = List(file123, other123, file11, file999, file10, file1234567890, other0)
    allFiles.foreach(_.createNewFile())

    fileTxLog.guessLogFile(0) should be(None)
    fileTxLog.guessLogFile(100) should be(None)
    fileTxLog.guessLogFile(101) should be(Some(file11))
    fileTxLog.guessLogFile(150) should be(Some(file11))
    fileTxLog.guessLogFile(199) should be(Some(file11))
    fileTxLog.guessLogFile(200) should be(Some(file11))
    fileTxLog.guessLogFile(201) should be(Some(file123))
    fileTxLog.guessLogFile(350) should be(Some(file999))
    fileTxLog.guessLogFile(400) should be(Some(file999))
    fileTxLog.guessLogFile(1000) should be(Some(file1234567890))

    // Create a couple of files without a consistent timestamp
    val file0 = new File(logDir, "service-0000001000-0:.log")
    val file1 = new File(logDir, "service-0000001000-1:.log")
    val newFiles = List(file0, file1)
    newFiles.foreach(_.createNewFile())

    fileTxLog.guessLogFile(0) should be(Some(file1))
    fileTxLog.guessLogFile(100) should be(Some(file1))
    fileTxLog.guessLogFile(101) should be(Some(file11))
  }

  test("should get next and previous log file") {
    val file10 = new File(logDir, "service-0000001000-10:.log")
    val file20 = new File(logDir, "service-0000001000-20:.log")
    val file30 = new File(logDir, "service-0000001000-30:.log")
    val other0 = new File(logDir, "service-0000009999-0.log") // Ignored
    val other123 = new File(logDir, "service-0000009999-123.log") // Ignored

    // Verify when no log file exist
    fileTxLog.getNextLogFile(file10) should be(None)
    fileTxLog.getPrevLogFile(file10) should be(None)

    val allFiles = List(file30, other123, file20, file10, other0)
    allFiles.foreach(_.createNewFile())

    fileTxLog.getNextLogFile(file10) should be(Some(file20))
    fileTxLog.getNextLogFile(file20) should be(Some(file30))
    fileTxLog.getNextLogFile(file30) should be(None)
    fileTxLog.getNextLogFile(other0) should be(None)

    fileTxLog.getPrevLogFile(file10) should be(None)
    fileTxLog.getPrevLogFile(file20) should be(Some(file10))
    fileTxLog.getPrevLogFile(file30) should be(Some(file20))
    fileTxLog.getPrevLogFile(other123) should be(None)
  }

  test("should not get any log files when there are no log files") {
    // No file at all
    fileTxLog.getLogFiles.toList should be(List[File]())
    fileTxLog.getLogFiles(Index(0)).toList should be(List[File]())

    // No files matching this token
    new File(logDir, "service-0000009999-0:0.log").createNewFile()
    new File(logDir, "service-0000009999-123:321.log").createNewFile()
    fileTxLog.getLogFiles.toList should be(List[File]())
    fileTxLog.getLogFiles(Index(0)).toList should be(List[File]())
  }

  test("should create tx logger even if log directory does not exist") {
    val fakeLogDir = new File("fakepath/1234567")
    fakeLogDir.isDirectory should be(false)

    val txLog = createFileTransactionLog(dir = fakeLogDir.getAbsolutePath)
    txLog.getLastLoggedRecord should be(None)
  }

  test("should append new record") {
    Range(0, 10).foreach(i => {
      fileTxLog.append(LogRecord(id = i, None, createRequestMessage(timestamp = i)))
    })

    val files = logDir.list()
    files should be(Array("service-0000001000-0:.log"))
  }

  test("should read record") {
    val record = LogRecord(id = 0, None, createRequestMessage(timestamp = 0))
    fileTxLog.append(record)
    fileTxLog.commit()

    val it = fileTxLog.read
    it.hasNext should be(true)
    it.next() should be(record)
    it.hasNext should be(false)
    it.close()
  }

  test("should append to non empty log directory") {
    val r1 = fileTxLog.append(LogRecord(id = 74, None, createRequestMessage(timestamp = 74)))
    val r2 = fileTxLog.append(LogRecord(id = 4321, None, createRequestMessage(timestamp = 4321)))
    fileTxLog.commit()
    fileTxLog.close()

    fileTxLog = createFileTransactionLog()
    val r3 = fileTxLog.append(LogRecord(id = 9999, Some(2000), createRequestMessage(timestamp = 9999)))
    fileTxLog.commit()

    val files = logDir.list().sorted
    files should be(Array("service-0000001000-74:.log", "service-0000001000-9999:2000.log"))

    val actualRecords = fileTxLog.read.toList
    actualRecords should be(List(r1, r2, r3))
  }

  test("should fail when trying to append out of order id") {
    val r1 = fileTxLog.append(LogRecord(id = 100, None, createRequestMessage(timestamp = 150)))
    fileTxLog.commit()
    val logFile = new File(logDir, fileTxLog.getNameFromIndex(r1))
    val fileLenAfterRecord1 = logFile.length()
    fileLenAfterRecord1 should be > 0L

    // Append a record with id less than previous record
    evaluating {
      fileTxLog.append(LogRecord(id = 99, None, createRequestMessage(timestamp = 151)))
    } should produce[IllegalArgumentException]
    fileTxLog.commit()
    logFile.length() should be(fileLenAfterRecord1)

    val r2 = fileTxLog.append(LogRecord(id = 101, None, createRequestMessage(timestamp = 152)))
    fileTxLog.commit()
    logFile.length() should be > fileLenAfterRecord1

    val actualrecords = fileTxLog.read.toList
    actualrecords should be(List(r1, r2))
  }

  test("should fail when trying to append out of order consistent timestamp") {
    val r1 = LogRecord(id = 100, Some(99), createRequestMessage(timestamp = 150))
    val r2 = LogRecord(id = 101, Some(0), createRequestMessage(timestamp = 151)) // Bad
    val r3 = LogRecord(id = 102, Some(99), createRequestMessage(timestamp = 152))
    val logFile = new File(logDir, fileTxLog.getNameFromIndex(r1))

    fileTxLog.append(r1)
    fileTxLog.commit()
    val fileLenAfterRecord1 = logFile.length()
    fileLenAfterRecord1 should be > 0L

    // Append a record with consistent timestamp not matching
    evaluating {
      fileTxLog.append(r2)
    } should produce[IllegalArgumentException]
    fileTxLog.commit()
    logFile.length() should be(fileLenAfterRecord1)

    fileTxLog.append(r3)
    fileTxLog.commit()
    logFile.length() should be > fileLenAfterRecord1

    val actualrecords = fileTxLog.read.toList
    actualrecords should be(List(r1, r3))
  }

  test("should fail when trying to append no consistent timestamp after a valid consistent timestamp") {
    val r1 = LogRecord(id = 100, Some(99), createRequestMessage(timestamp = 150))
    val r2 = LogRecord(id = 101, None, createRequestMessage(timestamp = 151)) // Bad
    val r3 = LogRecord(id = 102, Some(99), createRequestMessage(timestamp = 152))
    val logFile = new File(logDir, fileTxLog.getNameFromIndex(r1))

    fileTxLog.append(r1)
    fileTxLog.commit()
    val fileLenAfterRecord1 = logFile.length()
    fileLenAfterRecord1 should be > 0L

    // Append a record with consistent timestamp not matching
    evaluating {
      fileTxLog.append(r2)
    } should produce[IllegalArgumentException]
    fileTxLog.commit()
    logFile.length() should be(fileLenAfterRecord1)

    fileTxLog.append(r3)
    fileTxLog.commit()
    logFile.length() should be > fileLenAfterRecord1

    val actualrecords = fileTxLog.read.toList
    actualrecords should be(List(r1, r3))
  }

  test("should not corrupt transaction log when append fail due to transaction event persistence error") {
    val r1 = LogRecord(id = 100, None, createRequestMessage(timestamp = 0))
    val r2 = LogRecord(id = 101, None, createRequestMessage(timestamp = 1)) // Error
    when(spySerializer.serialize(r2)).thenThrow(new IOException("Forced error"))
    val r3 = LogRecord(id = 102, None, createRequestMessage(timestamp = 2))
    val logFile = new File(logDir, fileTxLog.getNameFromIndex(r1))

    // Append successful
    fileTxLog.append(r1)
    fileTxLog.commit()
    val fileLenAfterRecord1 = logFile.length()
    fileLenAfterRecord1 should be > 0L

    // Append error on transaction serialization, should not  modify the log file
    evaluating {
      fileTxLog.append(r2)
    } should produce[IOException]
    fileTxLog.commit()
    logFile.length() should be(fileLenAfterRecord1)

    // Append successful again
    fileTxLog.append(r3)
    fileTxLog.commit()
    logFile.length() should be > fileLenAfterRecord1

    val actualRecords = fileTxLog.read.toList
    actualRecords should be(List(r1, r3))
  }

  ignore("append from a new instance when an empty log file with the same name exist") {
    fail("Not implemented yet!")
  }

  test("should roll log files") {
    Range(0, 10).foreach(i => {
      fileTxLog.append(LogRecord(id = i, None, createRequestMessage(timestamp = i)))
      if ((i + 1) % 4 == 0) {
        fileTxLog.rollLog()
      }
    })

    val files = logDir.list().sorted
    files should be(Array("service-0000001000-0:.log", "service-0000001000-4:.log", "service-0000001000-8:.log"))
  }

  test("multiple roll log calls between append should not fail") {
    val r1 = fileTxLog.append(LogRecord(id = 100, None, createRequestMessage(timestamp = 0)))
    fileTxLog.rollLog()
    fileTxLog.rollLog()
    fileTxLog.rollLog()
    val r2 = fileTxLog.append(LogRecord(id = 101, None, createRequestMessage(timestamp = 1)))

    val files = logDir.list().sorted
    files should be(Array("service-0000001000-100:.log", "service-0000001000-101:.log"))
    fileTxLog.read.toList should be(List(r1, r2))
  }

  test("should automatically roll log file at a given size threshold") {
    val index = fileTxLog.append(Index(id = 100, None))
    val logFile = new File(logDir, fileTxLog.getNameFromIndex(index))
    val indexLen = logFile.length()

    // Compute file size required to rollover after appending an Index, a Request and 3 more Request records
    val r1 = fileTxLog.append(LogRecord(id = 101, None, createRequestMessage(timestamp = 1)))
    fileTxLog.commit()
    val setupLen = logFile.length()
    val recordLen = setupLen - indexLen
    val rolloverSize = setupLen + recordLen * 3 - recordLen / 2

    // Recreate transaction log with computed rollover size
    fileTxLog.close()
    logFile.delete()
    logFile.exists() should be(false)
    fileTxLog = createFileTransactionLog(fileRolloverSize = rolloverSize.toInt)
    fileTxLog.append(index)
    fileTxLog.append(r1)
    logFile.length() should be(setupLen)

    // Add 4 Request records, should rollover AFTER the 3rd
    fileTxLog.append(LogRecord(id = 201, None, createRequestMessage(timestamp = 11)))
    logDir.list().size should be(1)
    fileTxLog.append(LogRecord(id = 202, None, createRequestMessage(timestamp = 12)))
    logDir.list().size should be(1)
    fileTxLog.append(LogRecord(id = 203, None, createRequestMessage(timestamp = 13)))
    fileTxLog.commit()
    logDir.list().size should be(1)
    logFile.length() should be > rolloverSize
    fileTxLog.append(LogRecord(id = 204, None, createRequestMessage(timestamp = 14)))
    logDir.list().sorted should be(Array("service-0000001000-100:.log", "service-0000001000-204:.log"))
  }

  test("should read transactions from specified id") {
    fileTxLog.append(LogRecord(id = 0, None, createRequestMessage(timestamp = 0)))
    fileTxLog.append(LogRecord(id = 1, None, createRequestMessage(timestamp = 1)))
    fileTxLog.append(LogRecord(id = 2, None, createRequestMessage(timestamp = 2)))
    fileTxLog.append(LogRecord(id = 3, None, createRequestMessage(timestamp = 3)))
    fileTxLog.rollLog()
    fileTxLog.append(LogRecord(id = 4, None, createRequestMessage(timestamp = 4)))
    fileTxLog.append(LogRecord(id = 5, None, createRequestMessage(timestamp = 5)))
    fileTxLog.append(LogRecord(id = 6, None, createRequestMessage(timestamp = 6)))
    fileTxLog.append(LogRecord(id = 7, None, createRequestMessage(timestamp = 7)))
    fileTxLog.rollLog()
    fileTxLog.append(LogRecord(id = 8, None, createRequestMessage(timestamp = 8)))
    fileTxLog.append(LogRecord(id = 9, None, createRequestMessage(timestamp = 9)))
    fileTxLog.commit()

    // Validate starting at id 5 (which is also index 5)
    val it = fileTxLog.read(Index(5))
    5.until(10).foreach(i => {
      it.hasNext should be(true)
      it.next() should be(LogRecord(id = i, None, createRequestMessage(timestamp = i)))
    })
    it.hasNext should be(false)
    it.close()

    // Validate starting from every index
    0.until(10).foreach(i => {
      val expectedRecords = i.until(10).map(i => LogRecord(id = i, None, createRequestMessage(timestamp = i))).toList
      val actualRecords = fileTxLog.read(Index(i)).toList
      actualRecords should be(expectedRecords)
    })

    // Validate starting beyond end
    fileTxLog.read(Index(9999)).toList should be(List())
  }

  test("should read all transactions when no position specified") {
    val r1 = fileTxLog.append(LogRecord(id = 0, Some(100), createRequestMessage(timestamp = 1000)))
    val r2 = fileTxLog.append(LogRecord(id = 1, Some(200), createRequestMessage(timestamp = 1001)))
    val r3 = fileTxLog.append(LogRecord(id = 2, Some(300), createRequestMessage(timestamp = 1002)))
    fileTxLog.commit()

    // Validate starting from begining
    val actualRecords = fileTxLog.read.toList
    actualRecords should be(List(r1, r2, r3))
  }

  test("should read transactions from specified timestamp") {
    val r0 = fileTxLog.append(LogRecord(id = 0, None, createRequestMessage(timestamp = 0)))
    val r1 = fileTxLog.append(LogRecord(id = 1, None, createRequestMessage(timestamp = 100)))
    val r2 = fileTxLog.append(LogRecord(id = 2, Some(100), createRequestMessage(timestamp = 200)))
    fileTxLog.rollLog()
    val r3 = fileTxLog.append(LogRecord(id = 3, Some(200), createRequestMessage(timestamp = 300)))
    fileTxLog.rollLog()
    val r4 = fileTxLog.append(LogRecord(id = 4, Some(200), createRequestMessage(timestamp = 400)))
    val r5 = fileTxLog.append(LogRecord(id = 5, Some(200), createRequestMessage(timestamp = 500)))
    fileTxLog.rollLog()
    val r6 = fileTxLog.append(LogRecord(id = 6, Some(200), createRequestMessage(timestamp = 600)))
    val r7 = fileTxLog.append(LogRecord(id = 7, Some(300), createRequestMessage(timestamp = 700)))
    fileTxLog.rollLog()
    val r8 = fileTxLog.append(LogRecord(id = 8, Some(300), createRequestMessage(timestamp = 800)))
    val r9 = fileTxLog.append(LogRecord(id = 9, Some(300), createRequestMessage(timestamp = 900)))
    fileTxLog.commit()

    fileTxLog.read(Timestamp(0)).toList should be(List(r0, r1, r2, r3, r4, r5, r6, r7, r8, r9))
    fileTxLog.read(Timestamp(100)).toList should be(List(r1, r2, r3, r4, r5, r6, r7, r8, r9))
    fileTxLog.read(Timestamp(200)).toList should be(List(r2, r3, r4, r5, r6, r7, r8, r9))
    fileTxLog.read(Timestamp(300)).toList should be(List(r3, r4, r5, r6, r7, r8, r9))
    fileTxLog.read(Timestamp(400)).toList should be(List(r4, r5, r6, r7, r8, r9))
    fileTxLog.read(Timestamp(500)).toList should be(List(r5, r6, r7, r8, r9))
    fileTxLog.read(Timestamp(600)).toList should be(List(r6, r7, r8, r9))
    fileTxLog.read(Timestamp(700)).toList should be(List(r7, r8, r9))
    fileTxLog.read(Timestamp(800)).toList should be(List(r8, r9))
    fileTxLog.read(Timestamp(900)).toList should be(List(r9))

    // Beyond end
    fileTxLog.read(Timestamp(9999)).toList should be(List())

    // Unknown timestamp
    fileTxLog.read(Timestamp(-1)).toList should be(List())
    fileTxLog.read(Timestamp(850)).toList should be(List())
  }

  test("read should skip empty log files") {
    val r1 = LogRecord(id = 100, Some(100), createRequestMessage(timestamp = 1000)) // Good
    val fileRecord1 = new File(logDir, fileTxLog.getNameFromIndex(r1))

    val r2 = LogRecord(id = 150, Some(200), createRequestMessage(timestamp = 1001)) // Good
    val fileRecord2 = new File(logDir, fileTxLog.getNameFromIndex(r2))

    val r3 = LogRecord(id = 200, Some(300), createRequestMessage(timestamp = 1002)) // Bad
    when(spySerializer.serialize(r3)).thenThrow(new IOException("Forced error"))
    val fileRecord3 = new File(logDir, fileTxLog.getNameFromIndex(r3))

    val r4 = LogRecord(id = 250, Some(400), createRequestMessage(timestamp = 1003)) // Good
    val fileRecord4 = new File(logDir, fileTxLog.getNameFromIndex(r4))

    // Append first record
    fileTxLog.append(r1)
    fileTxLog.commit()
    fileRecord1.length() should be > 0L

    // Create an empty log file
    new File(logDir, fileTxLog.getNameFromIndex(Index(125, Some(100)))).createNewFile()

    // This append create a new log file
    fileTxLog.rollLog()
    fileTxLog.append(r2)
    fileTxLog.commit()
    fileRecord2.length() should be > 0L

    // Append again on a new log file but serialization fail, should result on a log containing only file headers
    fileTxLog.rollLog()
    evaluating {
      fileTxLog.append(r3)
    } should produce[IOException]
    fileTxLog.commit()
    fileRecord3.length() should be > 0L
    fileRecord3.length() should be < fileRecord2.length()

    // Append final transaction in a new log file
    fileTxLog.rollLog()
    fileTxLog.append(r4)
    fileTxLog.commit()
    fileRecord4.length() should be > 0L

    val expectedFiles = Array("service-0000001000-100:100.log", "service-0000001000-125:100.log",
      "service-0000001000-150:200.log", "service-0000001000-200:300.log", "service-0000001000-250:400.log")
    val actualFiles = logDir.list().sorted
    actualFiles should be(expectedFiles)

    val actualRecords = fileTxLog.read.toList
    actualRecords should be(List(r1, r2, r4))
  }

  test("read corrupted transaction event stop at corrupted tx") {
    val r1 = fileTxLog.append(LogRecord(id = 1, None, createRequestMessage(timestamp = 1000)))
    val r2 = fileTxLog.append(LogRecord(id = 2, None, createRequestMessage(timestamp = 1001)))
    val r3 = fileTxLog.append(LogRecord(id = 3, None, createRequestMessage(timestamp = 1002)))
    val r4 = fileTxLog.append(LogRecord(id = 4, None, createRequestMessage(timestamp = 1003)))
    fileTxLog.rollLog()
    val r5 = fileTxLog.append(LogRecord(id = 5, None, createRequestMessage(timestamp = 1004)))
    fileTxLog.commit()

    // Fail deserialization of the fourth transaction
    doReturn(r1).doReturn(r2).doReturn(r3).doThrow(new IOException()).doReturn(r5).when(spySerializer).deserialize(anyObject())

    // Start at second transaction
    val actualRecords = fileTxLog.read(Index(2)).toList
    actualRecords should be(List(r2, r3))
  }

  test("read corrupted transaction file header should stop reading at currupted file") {
    val r1 = fileTxLog.append(LogRecord(id = 10, None, createRequestMessage(timestamp = 1000)))
    val r2 = fileTxLog.append(LogRecord(id = 20, None, createRequestMessage(timestamp = 1001)))
    fileTxLog.rollLog()
    val r3 = fileTxLog.append(LogRecord(id = 30, None, createRequestMessage(timestamp = 1002)))
    fileTxLog.commit()

    // Create a log file between record2 and record3 with random content
    val buff = new Array[Byte](25)
    Random.nextBytes(buff)
    Files.write(new File(logDir, fileTxLog.getNameFromIndex(Index(25, None))).toPath, buff)

    val expectedFiles = Array("service-0000001000-10:.log", "service-0000001000-25:.log", "service-0000001000-30:.log")
    val actualFiles = logDir.list().sorted
    actualFiles should be(expectedFiles)

    val actualRecords = fileTxLog.read.toList
    actualRecords should be(List(r1, r2))
  }

  test("read transaction file with invalid header values") {

    // Create an empty log file with specified header values
    def createEmptyLogFileWithHeader(id: Long, magic: Long = FileTransactionLog.LogFileMagic,
                                     version: Int = FileTransactionLog.LogFileVersion, service: String = fileTxLog.service) {
      val baos = new ByteArrayOutputStream()
      val dos = new DataOutputStream(baos)

      dos.writeLong(magic)
      dos.writeInt(version)
      dos.writeUTF(service)
      dos.flush()
      baos.toByteArray

      Files.write(new File(logDir, fileTxLog.getNameFromIndex(Index(id, None))).toPath, baos.toByteArray)
    }

    // Create a valid log file with a single record.
    val record = fileTxLog.append(LogRecord(id = 10, None, createRequestMessage(timestamp = 1000)))
    fileTxLog.commit()

    // Ensure we can successfully create a log file with valid header
    createEmptyLogFileWithHeader(id = 0)
    fileTxLog.read.toList should be(List(record))

    // Invalid header magic
    createEmptyLogFileWithHeader(id = 0, magic = Random.nextLong())
    fileTxLog.read.toList should be(List())

    // Invalid header version
    createEmptyLogFileWithHeader(id = 0, version = FileTransactionLog.LogFileVersion + 1)
    fileTxLog.read.toList should be(List())

    // Invalid header service
    createEmptyLogFileWithHeader(id = 0, service = "dummy")
    fileTxLog.read.toList should be(List())

    // Test again with a valid header
    createEmptyLogFileWithHeader(id = 0)
    fileTxLog.read.toList should be(List(record))
  }

  test("should get the last logged timestamp") {
    fileTxLog.append(LogRecord(id = 0, None, createRequestMessage(timestamp = 0)))
    fileTxLog.rollLog()
    fileTxLog.append(LogRecord(id = 3, None, createRequestMessage(timestamp = 1)))
    fileTxLog.append(LogRecord(id = 4, Some(2000), createRequestMessage(timestamp = 2000)))
    fileTxLog.rollLog()
    fileTxLog.append(LogRecord(id = 999, Some(2000), createRequestMessage(timestamp = 2001)))
    val last: Index = fileTxLog.append(LogRecord(id = 9999, Some(2000), createRequestMessage(timestamp = 2002)))
    fileTxLog.commit()

    fileTxLog.getLastLoggedRecord should be(Some(last))

    // Close and try with a brand new instance
    fileTxLog.close()
    fileTxLog = createFileTransactionLog()
    fileTxLog.getLastLoggedRecord should be(Some(last))
  }

  test("the last logged index should be None when there are no log files") {
    fileTxLog.getLastLoggedRecord should be(None)
  }

  test("should get the last logged index even if last log file is empty") {
    val last: Index = fileTxLog.append(LogRecord(id = 100, None, createRequestMessage(timestamp = 0)))
    fileTxLog.commit()

    // Create an empty log file
    new File(logDir, fileTxLog.getNameFromIndex(Index(200, Some(199)))).createNewFile()

    val expectedFiles = Array("service-0000001000-100:.log", "service-0000001000-200:199.log")
    val actualFiles = logDir.list().sorted
    actualFiles should be(expectedFiles)

    fileTxLog.getLastLoggedRecord should be(Some(last))

    // Close and try with a brand new instance
    fileTxLog.close()
    fileTxLog = createFileTransactionLog()
    fileTxLog.getLastLoggedRecord should be(Some(last))
  }

  test("should get the last logged index even if last log file contains file header only") {
    val record1 = LogRecord(id = 100, None, createRequestMessage(timestamp = 0))
    val fileRecord1 = new File(logDir, fileTxLog.getNameFromIndex(record1))
    val record2 = LogRecord(id = 200, Some(100), createRequestMessage(timestamp = 120))
    when(spySerializer.serialize(record2)).thenThrow(new IOException())
    val fileRecord2 = new File(logDir, fileTxLog.getNameFromIndex(record2))

    fileTxLog.append(record1)
    fileTxLog.rollLog()

    // record2 serialization should fail, resulting to a log file containing only file headers
    fileTxLog.rollLog()
    evaluating {
      fileTxLog.append(record2)
    } should produce[IOException]
    fileTxLog.commit()
    fileRecord2.length() should be > 0L
    fileRecord2.length() should be < fileRecord1.length()

    val expectedFiles = Array("service-0000001000-100:.log", "service-0000001000-200:100.log")
    val actualFiles = logDir.list().sorted
    actualFiles should be(expectedFiles)

    fileTxLog.getLastLoggedRecord should be(Some(record1))

    // Close and try with a brand new instance
    fileTxLog.close()
    fileTxLog = createFileTransactionLog()
    fileTxLog.getLastLoggedRecord should be(Some(record1))
  }

  test("truncate should delete all records from the specified location") {
    val r0 = fileTxLog.append(LogRecord(id = 0, Some(100), createRequestMessage(timestamp = 1000)))
    val r1 = fileTxLog.append(LogRecord(id = 1, Some(200), createRequestMessage(timestamp = 1001)))
    val r2 = fileTxLog.append(LogRecord(id = 2, Some(300), createRequestMessage(timestamp = 1002)))
    val r3 = fileTxLog.append(LogRecord(id = 3, Some(400), createRequestMessage(timestamp = 1003)))
    fileTxLog.rollLog()
    val r4 = fileTxLog.append(LogRecord(id = 4, Some(599), createRequestMessage(timestamp = 1004)))
    val r5 = fileTxLog.append(LogRecord(id = 5, Some(600), createRequestMessage(timestamp = 1005)))
    val r6 = fileTxLog.append(LogRecord(id = 6, Some(600), createRequestMessage(timestamp = 1006)))
    val r7 = fileTxLog.append(LogRecord(id = 7, Some(601), createRequestMessage(timestamp = 1007)))
    fileTxLog.rollLog()
    val r8 = fileTxLog.append(LogRecord(id = 8, Some(601), createRequestMessage(timestamp = 1008)))
    fileTxLog.rollLog()
    val r9 = fileTxLog.append(LogRecord(id = 9, Some(700), createRequestMessage(timestamp = 1009)))
    fileTxLog.commit()

    fileTxLog.read.toList should be(List(r0, r1, r2, r3, r4, r5, r6, r7, r8, r9))
    logDir.list().sorted should be(Array("service-0000001000-0:100.log", "service-0000001000-4:599.log",
      "service-0000001000-8:601.log", "service-0000001000-9:700.log"))

    fileTxLog.truncate(r6)

    logDir.list().sorted should be(Array("service-0000001000-0:100.log", "service-0000001000-4:599.log"))
    fileTxLog.read.toList should be(List(r0, r1, r2, r3, r4, r5))
    fileTxLog.read(Index(r6.id)).toList should be(List())
    fileTxLog.read(Index(r6.id)).toList should be(List())
    fileTxLog.read(Timestamp(1005)).toList should be(List(r5))
  }

  ignore("commit") {
    fail("Not implemented yet!")
  }

  ignore("close") {
    fail("Not implemented yet!")
  }
}
