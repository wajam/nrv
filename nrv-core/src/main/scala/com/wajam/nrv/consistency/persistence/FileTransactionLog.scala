package com.wajam.nrv.consistency.persistence

import com.wajam.nrv.utils.timestamp.Timestamp
import com.wajam.nrv.Logging
import com.yammer.metrics.scala.Instrumented
import java.io._
import com.wajam.nrv.consistency.persistence.LogRecord._
import java.util.zip.CRC32
import java.nio.ByteBuffer
import com.wajam.nrv.utils.PositionInputStream

/**
 * Class for writing and reading transaction logs.
 *
 * <p>
 * The format of a Transactional log is as follows:
 * <blockquote><pre>
 * LogFile:
 * FileHeader RecordList
 *
 * FileHeader: {
 * magic 8bytes "NRVTXLOG"
 * version 4bytes
 * servicename mutf-8
 * }
 *
 * RecordList:
 * Record || Record RecordList
 *
 * Record:
 * checksum recordlen type Request | Response | Index 0x5A
 *
 * checksum: 8bytes CRC32 calculated across recordlen, type, Request, Reponse, Index and 0x5A
 * recordlen: 4 bytes
 * type: 2bytes (1=Request, 2=Response, 3=Index)
 *
 * Request: {
 * id 8bytes
 * consistenttimestamp 8bytes
 * timestamp 8bytes
 * token 8bytes
 * msglen 4bytes
 * message
 * }
 *
 * Response: {
 * id 8bytes
 * consistenttimestamp 8bytes
 * timestamp 8bytes
 * token 8bytes
 * status 2bytes (1=OK, 0=Error)
 * }
 *
 * Index: {
 * id 8bytes
 * consistenttimestamp 8bytes
 * }
 *
 * </pre></blockquote>
 */
class FileTransactionLog(val service: String, val token: Long, val logDir: String, fileRolloverSize: Int = 0,
                         serializer: LogRecordSerializer = new LogRecordSerializer)
  extends TransactionLog with Logging with Instrumented {

  import FileTransactionLog._

  lazy private val appendTimer = metrics.timer("append-time")
  lazy private val truncateTimer = metrics.timer("truncate-time")
  lazy private val commitTimer = metrics.timer("commit-time")
  lazy private val commitSyncTimer = metrics.timer("commit-sync-time")
  lazy private val rollMeter = metrics.meter("roll-calls", "roll-calls")

  lazy private val readIndexTimer = metrics.timer("read-index-time")
  lazy private val readTimestampTimer = metrics.timer("read-timestamp-time")
  lazy private val readNextCalls = metrics.meter("read-next-calls", "read-next-calls")
  lazy private val readNextError = metrics.meter("read-next-error", "read-next-error")
  lazy private val readNextTimer = metrics.timer("read-next-time")
  lazy private val readOpenMeter = metrics.meter("read-open-calls", "read-open-calls")

  // TODO: Add write-bytes and read-bytes meters

  private val filePrefix = "%s-%010d".format(service, token)

  private var fileStreams: List[FileOutputStream] = List()
  private var logStream: Option[DataOutputStream] = None
  private var writeFile: File = null

  private var lastIndex: Option[Index] = getLastLoggedRecord.map(r => Index(r.id, r.consistentTimestamp))

  override def toString = {
    "service=%s, tk=%d, ts=%s, open=%s".format(service, token, lastIndex.getOrElse(""), writeFile)
  }

  /**
   * Returns the most recent consistent timestamp written on the log storage.
   */
  def getLastLoggedRecord: Option[LogRecord] = synchronized {
    // Read the last timestamp from the last file. If the file is empty, do the same same the previous one and so on
    // until a timestamp is found.
    var result: Option[LogRecord] = None
    getLogFiles.toList.reverse.find(file => {
      val fileIndex = getIndexFromName(file.getName)
      val it = read(fileIndex)
      try {
        it.toIterable.lastOption match {
          case Some(record) => {
            result = Some(record)
            true
          }
          case _ => false
        }
      } finally {
        it.close()
      }
    })

    result
  }

  /**
   * Appends the specified record to the transaction log
   */
  def append[T <: LogRecord](block: => T): T = {

    def validateRecord(record: T) {
      lastIndex match {
        case Some(Index(lastId, lastTimestampOpt)) => {
          // Ensure record id is > than last index id
          require(record.id > lastId, "This record id %s <= previous index id %s".format(record.id, lastId))

          // Ensure record consistent timestamp is >= than last index consistent timestamp
          (record.consistentTimestamp, lastTimestampOpt) match {
            case (Some(recordTimestamp), Some(lastTimestamp)) => {
              require(recordTimestamp >= lastTimestamp,
                "This record consistent timestamp %s < previous index consistent timestamp %s".format(
                  recordTimestamp, lastTimestamp))
            }
            case (None, Some(lastTimestamp)) => throw new IllegalArgumentException(
              "This record has no consistent timestamp while previous index consistent timestamp is %s".format(
                lastTimestamp))
            case _ => // No timestamp to validate
          }
        }
        case None => // Nothing to validate
      }
    }

    appendTimer.time {
      this.synchronized {
        val record: T = block
        validateRecord(record)

        // Write the transaction event into the open log file
        writeRecord(logStream.getOrElse(openNewLogFile(record)), record)
        lastIndex = Some(record)

        // Roll log if needed
        if (fileRolloverSize > 0) {
          logStream match {
            case Some(out) if (out.size >= fileRolloverSize) => rollLog()
            case _ => // No need to roll
          }
        }

        record
      }
    }
  }

  private def openNewLogFile(index: Index) = {
    val file = new File(logDir, getNameFromIndex(index))
    info("Creating new log file: {}", file)
    val fos = new FileOutputStream(file)
    val dos = new DataOutputStream(new BufferedOutputStream(fos))
    writeFileHeader(dos)

    logStream = Some(dos)
    fileStreams = fos :: fileStreams
    writeFile = file
    dos
  }

  private def writeFileHeader(dos: DataOutputStream) {
    dos.writeLong(LogFileMagic)
    dos.writeInt(LogFileVersion)
    dos.writeUTF(service)
    dos.flush()
  }

  private def writeRecord(dos: DataOutputStream, record: LogRecord) {
    val crc = new CRC32()
    val buf = serializer.serialize(record)
    crc.update(buf.length)
    crc.update(buf, 0, buf.length)
    crc.update(EOT)

    dos.writeLong(crc.getValue)
    dos.writeInt(buf.length)
    dos.write(buf)
    dos.writeInt(EOT)
    dos.flush()
  }

  /**
   * Read all records from the begining of the log
   */
  def read: TransactionLogIterator = {
    read(Index(Long.MinValue))
  }

  /**
   * Read all the records from the specified index
   */
  def read(index: Index): TransactionLogIterator = {
    readIndexTimer.time {
      new FileTransactionLogIterator(index)
    }
  }

  /**
   * Read all the records from the specified request record timestamp. Returns an empty iterator if no request record
   * with the specified timestamp is found. The implementation tries its best to read directly from the proper log
   * file but have to read from extra log file if required. At worst all the log files may be read to locate the first
   * record if no record exists for the specified timestamp.
   */
  def read(timestamp: Timestamp): TransactionLogIterator = {
    readTimestampTimer.time {
      // Try to guess in which log file the searched record is written. This is the last possible log file. If the
      // record is not found in this file, read the previous log file and so on until the record is found or the first
      // log is scanned.
      var file: Option[File] = guessLogFile(timestamp)
      var result: Option[TransactionLogIterator] = None
      while (result.isEmpty && file.isDefined) {
        findRequestInFile(timestamp, file.get) match {
          case (Some(record), it) => {
            // Found the initial record, reuse the iterator used to search the record
            result = Some(new CompositeTransactionLogIterator(record, it))
          }
          case (None, _) => {
            // Record not found the log file, try with previous log file
            file = getPrevLogFile(file)
          }
        }
      }

      result.getOrElse(EmptyTransactionLogIterator)
    }
  }

  /**
   * Search in the specified log file, the request record matching the specified timestamp. Returns a tuple with the
   * found record and the iterator used to do the search. The iterator is positioned at the record following the
   * found record.
   */
  private[persistence] def findRequestInFile(timestamp: Timestamp, logFile: File): (Option[Request], TransactionLogIterator) = {
    val nextFileFirstId = getNextLogFile(Some(logFile)).map(file =>
      getIndexFromName(file.getName)).map(_.id).getOrElse(Long.MaxValue)
    val it = new FileTransactionLogIterator(getIndexFromName(logFile.getName))
    var beyondMax = false
    var foundRequest: Option[Request] = None
    while (it.hasNext && !beyondMax && foundRequest.isEmpty) {
      it.next() match {
        case request: Request if request.timestamp == timestamp => foundRequest = Some(request)
        case record: LogRecord if record.id >= nextFileFirstId => beyondMax = true
        case _ =>
      }
    }
    (foundRequest, it)
  }

  /**
   * Truncate log storage from the specified index inclusively
   */
  def truncate(index: Index) {
    truncateTimer.time {
      this.synchronized {
        val it = new FileTransactionLogIterator(index)
        try {
          if (it.hasNext) {
            val record = it.nextInternal()
            it.close() // Close explicitly before truncate

            // Now truncate at the current position
            val raf = new RandomAccessFile(record.logFile, "rw")
            raf.setLength(record.position)
            raf.close()

            // Delete remaining log files
            getLogFiles(index).filter(file =>
              getIndexFromName(file.getName) > getIndexFromName(record.logFile.getName)).foreach(_.delete())
          }
        } finally {
          it.close()
        }
      }
    }
  }

  /**
   * Rollover the current log file
   */
  def rollLog() {
    this.synchronized {
      rollMeter.mark()

      logStream.foreach(_.flush())
      logStream = None
      writeFile = null
    }
  }

  /**
   * Ensure that transaction log is fully written on the log storage
   */
  def commit() {
    commitTimer.time {
      this.synchronized {

        // Flush streams
        logStream.foreach(_.flush())
        for (fos <- fileStreams) {
          fos.flush()
          commitSyncTimer.time {
            fos.getChannel.force(false)
          }
        }

        // Close all open streams but the most recent one
        fileStreams match {
          case fileStream :: toClose => {
            fileStreams = fileStream :: Nil
            toClose.foreach(_.close())
          }
          case Nil => // Nothing to close
        }
      }
    }
  }

  /**
   * Close this transaction log
   */
  def close() {
    this.synchronized {
      logStream.foreach(_.close())
      fileStreams.foreach(_.close())
      writeFile = null
    }
  }

  /**
   * Returns the log files in ascending order that contain transactions from the specified index.
   */
  def getLogFiles(index: Index): Iterable[File] = {
    val sortedFiles = getLogFiles.toIndexedSeq
    sortedFiles.indexWhere(file => getIndexFromName(file.getName).id > index.id) match {
      case -1 if sortedFiles.isEmpty => sortedFiles
      case -1 => List(sortedFiles.last)
      case 0 => sortedFiles
      case i => sortedFiles.drop(i - 1)
    }
  }

  /**
   * Returns all the log files sorted in ascending order
   */
  def getLogFiles: Iterable[File] = {
    val directory = new File(logDir)
    val logFiles = if (directory.exists()) {
      directory.listFiles(new FilenameFilter {
        def accept(dir: File, name: String) = {
          name.startsWith(filePrefix) && name.endsWith(".log")
        }
      })
    } else {
      Array[File]()
    }

    logFiles.sortWith((f1, f2) => getIndexFromName(f1.getName) < getIndexFromName(f2.getName))
  }

  /**
   * Returns the log file that most likely contain the record with the specified timestamp. The record is garantee to
   * NOT be in a following log file but may be in an earlier log file.
   */
  private[persistence] def guessLogFile(timestamp: Timestamp): Option[File] = {
    // Find last log file having a consistent timestamp less than searched timestamp
    getLogFiles.toSeq.reverseIterator.find(file =>
      getIndexFromName(file.getName).consistentTimestamp.getOrElse(Timestamp(Long.MinValue)) < timestamp)
  }

  /**
   * Returns the log file following the specified log file
   */
  private[persistence] def getNextLogFile(logFile: Option[File]): Option[File] = {
    logFile match {
      case Some(file) => {
        val remaining = getLogFiles.dropWhile(_ != file)
        remaining.toList match {
          case _ :: n :: _ => Some(n)
          case _ => None
        }
      }
      case None => None
    }
  }

  /**
   * Returns the log file preceding the sepcified log file
   */
  private[persistence] def getPrevLogFile(logFile: Option[File]): Option[File] = {
    logFile match {
      case Some(file) => {
        val remaining = getLogFiles.toSeq.reverseIterator.dropWhile(_ != file)
        remaining.toList match {
          case _ :: n :: _ => Some(n)
          case _ => None
        }
      }
      case None => None
    }
  }

  private[persistence] def getNameFromIndex(index: Index): String = {
    "%s-%d:%s.log".format(filePrefix, index.id, index.consistentTimestamp.getOrElse("").toString)
  }

  private[persistence] def getIndexFromName(name: String): Index = {
    val end = name.lastIndexOf(".")
    val start = name.lastIndexOf("-", end) + 1
    name.substring(start, end).split(":") match {
      case Array(id) => Index(id.toLong, None)
      case Array(id, timestamp) => Index(id.toLong, Some(Timestamp(timestamp.toLong)))
    }
  }

  private case class IteratorRecord(record: LogRecord, position: Long, logFile: File)

  private class FileTransactionLogIterator(initialIndex: Index) extends TransactionLogIterator {
    private var logStream: Option[DataInputStream] = None
    private var positionStream: PositionInputStream = null
    private var nextRecord: Either[Exception, Option[IteratorRecord]] = Right(None)
    private var readFile: File = null

    // Position the iterator to the initial timestamp by reading tx from log as long the read timestamp is before the
    // initial timestamp
    private val logFiles = getLogFiles(initialIndex).toIterator
    openNextFile()
    while (nextRecord match {
      case Right(Some(IteratorRecord(record, _, _))) => record.id < initialIndex.id
      case Left(e) => throw new IOException(e)
      case _ => false
    }) {
      readNextRecord()
    }

    def hasNext: Boolean = {
      nextRecord match {
        case Right(Some(_)) => true
        case _ => false
      }
    }

    def next(): LogRecord = {
      nextInternal().record
    }

    private[persistence] def nextInternal(): IteratorRecord = {
      readNextCalls.mark()

      val result = nextRecord match {
        case Right(Some(record)) => record
        case Left(e) => throw new IOException(e)
        case _ => throw new IllegalStateException("Must not be invoked beyond the end of the iterator")
      }

      // Read ahead the next transaction for a future call
      readNextRecord()

      result
    }

    def close() {
      logStream.foreach(_.close())
      logStream = None
      positionStream = null
      readFile = null
    }

    private def openNextFile(): Boolean = {
      readOpenMeter.mark()

      logStream.foreach(_.close())
      logStream = None
      readFile = null

      if (logFiles.hasNext) {
        readFile = logFiles.next()
        if (readFile.length() == 0) {
          info("Skipping empty log file: {}", readFile)
          openNextFile()
        } else {
          info("Opening log file: {}", readFile)
          val pis = new PositionInputStream(new FileInputStream(readFile))
          val dis = new DataInputStream(pis)
          if (readFileHeader(dis)) {
            positionStream = pis
            logStream = Some(dis)
            readNextRecord()
          } else {
            nextRecord = Right(None)
            false
          }
        }
      } else {
        false
      }
    }

    private def readFileHeader(dis: DataInputStream): Boolean = {
      try {
        val readMagic = dis.readLong()
        if (readMagic != LogFileMagic) {
          throw new IOException("Log file header magic %x not %x".format(readMagic, LogFileMagic))
        }
        val readVersion = dis.readInt()
        if (readVersion != LogFileVersion) {
          throw new IOException("Unsupported log file version %d".format(readVersion))
        }
        val readService = dis.readUTF()
        if (readService != service) {
          throw new IOException("Service %s not %s".format(readService, service))
        }
        true
      } catch {
        case e: Exception => {
          readNextError.mark()
          warn("Error reading log file header from {}: {}", readFile, e)
          nextRecord = Left(e)
          false
        }
      }
    }

    private def readNextRecord(): Boolean = {
      readNextTimer.time {
        logStream match {
          case Some(dis) => {
            try {
              val position = positionStream.position
              val crc = dis.readLong() // TODO: crc validation
              try {
                val txLen = dis.readInt() // TODO: protection against negative or len too large
                val buf = new Array[Byte](txLen)
                dis.read(buf) // TODO: validate read len
                val record = serializer.deserialize(buf)
                val eot = dis.readInt() // TODO: validate eot
                nextRecord = Right(Some(IteratorRecord(record, position, readFile)))
                true
              } catch {
                case e: Exception => {
                  readNextError.mark()
                  warn("Error reading transaction from {}: {}", readFile, e)
                  nextRecord = Left(e)
                  false
                }
              }
            } catch {
              case e: EOFException => {
                info("End of log file: {}", readFile)
                nextRecord = Right(None)
                openNextFile()
              }
            }
          }
          case _ => false
        }
      }
    }
  }

}

object FileTransactionLog {
  val LogFileMagic: Long = ByteBuffer.wrap("NRVTXLOG".getBytes).getLong
  val LogFileVersion: Int = 1
  val EOT: Int = 0x5a
}
