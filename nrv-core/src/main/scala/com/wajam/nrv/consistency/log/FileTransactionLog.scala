package com.wajam.nrv.consistency.log

import com.wajam.nrv.Logging
import com.yammer.metrics.scala.Instrumented
import java.io._
import com.wajam.nrv.consistency.log.LogRecord._
import java.util.zip.CRC32
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import scala.annotation.tailrec
import com.wajam.commons.Closable
import com.wajam.nrv.utils.timestamp.Timestamp

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
 * skipinterval 4bytes
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
                         skipIntervalSize: Int = 4096 * 50,
                         serializer: Option[LogRecordSerializer] = None)
  extends TransactionLog with Logging with Instrumented {

  import FileTransactionLog._

  lazy private val lastLoggedRecordTimer = metrics.timer("last-logged-record-time")
  lazy private val appendTimer = metrics.timer("append-time")
  lazy private val truncateTimer = metrics.timer("truncate-time")
  lazy private val commitTimer = metrics.timer("commit-time")
  lazy private val commitSyncTimer = metrics.timer("commit-sync-time")
  lazy private val rollMeter = metrics.meter("roll-calls", "roll-calls")

  lazy private val readIndexTimer = metrics.timer("read-index-time")
  lazy private val readTimestampTimer = metrics.timer("read-timestamp-time")
  lazy private val readNextTimer = metrics.timer("read-next-time")
  lazy private val readOpenMeter = metrics.meter("read-open-calls", "read-open-calls")

  require(fileRolloverSize >= 0)
  require(skipIntervalSize > 0)

  private val recordSerializer = serializer.getOrElse(new LogRecordSerializer)

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
    lastLoggedRecordTimer.time {
      // Iterate log files in reverse order to find their last record. Stop at the first log file which contains a
      // record.
      getLogFiles.toList.reverse.toIterator.flatMap {
        case file =>
          val fileIndex = getIndexFromName(file.getName)
          var it = FileTransactionLogIterator(fileIndex)
          val record: Option[LogRecord] = try {
            // Try to skip to the last interval. It may fail if a record is larger than the actual interval size.
            // If it fails fallback to sequential read to the last record
            it.skipToCurrentLogFileLastInterval()
            it.toTransactionLogIterator.toIterable.lastOption
          } catch {
            case e: Exception => {
              info("Failed to skip to the last log file interval. Falling back to sequential read. {}", file)
              it.close()
              it = FileTransactionLogIterator(fileIndex)
              it.toTransactionLogIterator.toSafeIterator.toIterable.lastOption
            }
          } finally {
            it.close()
          }
          record
      }.collectFirst {
        case record: LogRecord => record
      }
    }
  }

  /**
   * Appends the specified record to the transaction log
   */
  def append[T <: LogRecord](block: => T): T = {

    def validateRecord(record: T) {
      // Ensure record timestamp is > than record consistent timestamp
      (record, record.consistentTimestamp) match {
        case (timestamped: TimestampedRecord, Some(consistentTimestamp)) => {
          require(timestamped.timestamp > consistentTimestamp,
            "This record timestamp %s <= record consistent timestamp %s".format(
              timestamped.timestamp, consistentTimestamp))
        }
        case _ => // No timestamp to validate
      }

      // Validate record vs previous record
      lastIndex match {
        case Some(Index(lastId, lastConsistentTimestampOpt)) => {
          // Ensure record id is > than last record id
          require(record.id > lastId, "This record id %s <= previous record id %s".format(record.id, lastId))

          // Ensure record consistent timestamp is >= than last record consistent timestamp
          (record.consistentTimestamp, lastConsistentTimestampOpt) match {
            case (Some(recordConsistentTimestamp), Some(lastConsistentTimestamp)) => {
              require(recordConsistentTimestamp >= lastConsistentTimestamp,
                "This record consistent timestamp %s < previous record consistent timestamp %s".format(
                  recordConsistentTimestamp, lastConsistentTimestamp))
            }
            case (None, Some(lastTimestamp)) => throw new IllegalArgumentException(
              "This record has no consistent timestamp while previous record consistent timestamp is %s".format(
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
            case Some(out) if out.size >= fileRolloverSize => rollLog()
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
    writeFileHeader(dos, FileHeader(LogFileMagic, LogFileCurVersion, service, skipIntervalSize))

    logStream = Some(dos)
    fileStreams = fos :: fileStreams
    writeFile = file
    dos
  }

  private def writeFileHeader(dos: DataOutputStream, header: FileHeader) {
    dos.writeLong(header.fileMagic)
    dos.writeInt(header.fileVersion)
    dos.writeUTF(header.fileService)
    dos.writeInt(header.fileSkipIntervalSize)
    dos.flush()
  }

  private def writeRecord(dos: DataOutputStream, record: LogRecord) {

    val buf = recordSerializer.serialize(record)

    // Pad up to the skip interval if the record size would overlap the interval
    val lenBeforeSkip = skipIntervalSize - dos.size() % skipIntervalSize
    if (lenBeforeSkip < buf.length + 16) {
      0.until(lenBeforeSkip).foreach(_ => dos.write(0))
    }

    dos.writeLong(computeRecordCrc(buf))
    dos.writeInt(buf.length)
    dos.write(buf)
    dos.writeInt(EOR) // End of record
    dos.flush()
  }

  private def computeRecordCrc(buffer: Array[Byte]): Long = {
    val crc = new CRC32()
    crc.update(buffer.length)
    crc.update(buffer, 0, buffer.length)
    crc.update(EOR)
    crc.getValue
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
      FileTransactionLogIterator(index).toTransactionLogIterator
    }
  }

  /**
   * Read all the records from the specified request record timestamp. This method throw a NoSuchElementException
   * if no request record with the specified timestamp is found.
   * <p></p>
   * The implementation tries its best to read directly from the proper log file but have to read from extra log file
   * if required. At worst all the log files may be read to locate the first record if no record exists for the
   * specified timestamp.
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

      result.get
    }
  }

  /**
   * Search in the specified log file, the request record matching the specified timestamp. Returns a tuple with the
   * found record and the iterator used to do the search. The iterator is positioned at the record following the
   * found record.
   */
  private[log] def findRequestInFile(timestamp: Timestamp, logFile: File): (Option[Request], TransactionLogIterator) = {
    val nextFileFirstId = getNextLogFile(Some(logFile)).map(file =>
      getIndexFromName(file.getName)).map(_.id).getOrElse(Long.MaxValue)
    val it = FileTransactionLogIterator(getIndexFromName(logFile.getName)).toTransactionLogIterator
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
        val it = FileTransactionLogIterator(index)
        try {
          if (it.hasNext) {
            val record = it.next()
            it.close() // Close explicitly before truncate

            // Delete log files following the specified index in reverse order
            val reversedRemainingFiles = getLogFiles(index).filter(file =>
              getIndexFromName(file.getName) > getIndexFromName(record.logFile.getName)).toSeq.reverse
            reversedRemainingFiles.foreach(_.delete())

            // Now truncate at the current position
            val raf = new RandomAccessFile(record.logFile, "rw")
            raf.setLength(record.position)
            raf.close()
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
  private[log] def guessLogFile(timestamp: Timestamp): Option[File] = {
    // Find last log file having a consistent timestamp less than searched timestamp
    getLogFiles.toSeq.reverseIterator.find(file =>
      getIndexFromName(file.getName).consistentTimestamp.getOrElse(Timestamp(Long.MinValue)) < timestamp)
  }

  /**
   * Returns the log file following the specified log file
   */
  private[log] def getNextLogFile(logFile: Option[File]): Option[File] = {
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
  private[log] def getPrevLogFile(logFile: Option[File]): Option[File] = {
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

  private[log] def getNameFromIndex(index: Index): String = {
    "%s-%d:%s.log".format(filePrefix, index.id, index.consistentTimestamp.getOrElse("").toString)
  }

  private[log] def getIndexFromName(name: String): Index = {
    val end = name.lastIndexOf(".")
    val start = name.lastIndexOf("-", end) + 1
    name.substring(start, end).split(":") match {
      case Array(id) => Index(id.toLong, None)
      case Array(id, timestamp) => Index(id.toLong, Some(Timestamp(timestamp.toLong)))
    }
  }

  private case class FileLogRecord(record: LogRecord, position: Long, logFile: File)

  private case class FileHeader(fileMagic: Long, fileVersion: Int, fileService: String, fileSkipIntervalSize: Int) {
    require(fileMagic == LogFileMagic, "Log file header magic %x not %x".format(fileMagic, LogFileMagic))
    require(fileVersion >= LogFileMinVersion & fileVersion <= LogFileCurVersion,
      "Unsupported log file version %d (min=%d, max=%d)".format(fileVersion, LogFileMinVersion, LogFileMaxVersion))
    require(fileService == service, "Service %s not %s".format(fileService, service))
  }

  private trait FileTransactionLogIterator extends Iterator[FileLogRecord] with Closable {
    def skipToCurrentLogFileLastInterval()

    def toTransactionLogIterator: TransactionLogIterator = {

      val it = this

      new TransactionLogIterator {
        def hasNext = it.hasNext

        def next() = it.next().record

        def close() = it.close()
      }
    }
  }

  private object FileTransactionLogIterator {

    /**
     * Creates a new FileTransactionLogIterator positioned at the specified initial Index.
     */
    def apply(initialIndex: Index): FileTransactionLogIterator = new FileTransactionLogIterator {
      // Flag to delay initialization as much as possible to use this iterator with TransactionLog safe iterator
      var initialized = false
      var initialRecord: Option[FileLogRecord] = None

      val it = new FileTransactionLogIteratorImpl(new LogFileIterator(FileTransactionLog.this, initialIndex))

      def hasNext = {
        ensureInitialized()
        initialRecord.isDefined || it.hasNext
      }

      def next() = {
        ensureInitialized()
        initialRecord match {
          case Some(fileRecord) => {
            initialRecord = None
            fileRecord
          }
          case None => it.next()
        }
      }

      def skipToCurrentLogFileLastInterval() = it.skipToCurrentLogFileLastInterval()

      def close() {
        it.close()
      }

      private def ensureInitialized() {
        if (!initialized) {
          // Position the iterator to the initial index. The initial record is temporary cached and returned at
          // the initial next() call.
          initialRecord = it.collectFirst {
            case fileRecord if initialIndex.id <= fileRecord.record.id => fileRecord
          }
          initialized = true
        }
      }
    }
  }

  private class FileTransactionLogIteratorImpl(logFileIterator: LogFileIterator) extends FileTransactionLogIterator {

    class OpenLogFile(val file: File, val header: FileHeader,
                      val dataStream: DataInputStream, fileStream: FileInputStream) {

      def channel: FileChannel = fileStream.getChannel

      def close() {
        dataStream.close()
      }
    }

    // Flag to delay initialization as much as possible to use this iterator with TransactionLog safe iterator
    private var initialized = false
    private val logFiles = logFileIterator.flatMap(openLogFile)
    private var currentLogfile: Option[OpenLogFile] = None

    def hasNext: Boolean = {
      ensureInitialized()

      (for (openFile <- currentLogfile) yield openFile.channel.position() < openFile.channel.size()) match {
        case Some(true) => true
        case Some(false) => logFiles.hasNext
        case None => false
      }
    }

    @tailrec
    final def next(): FileLogRecord = {
      ensureInitialized()

      val openFile = currentLogfile.get
      if (openFile.channel.position() < openFile.channel.size()) readNextTimer.time {
        // Haven't reach the end of the current log file, read the record from the current log file
        val position = openFile.channel.position()
        val fileSkipIntervalSize = openFile.header.fileSkipIntervalSize
        val lenBeforeSkip = fileSkipIntervalSize - position % fileSkipIntervalSize
        if (lenBeforeSkip <= 12) {
          // Crc and record length take 12 bytes. If there is less bytes available before the next skip interval,
          // go immediately to the beginning of the next skip interval.
          openFile.dataStream.skip(lenBeforeSkip)
        }

        var crc = openFile.dataStream.readLong()
        var recordLen = openFile.dataStream.readInt()

        // Detect padding to next skip interval
        if (crc == 0 && recordLen == 0) {
          require(lenBeforeSkip > 12)
          openFile.dataStream.skip(lenBeforeSkip - 12)
          crc = openFile.dataStream.readLong()
          recordLen = openFile.dataStream.readInt()
        }

        require(recordLen >= MinRecordLen && recordLen <= MaxRecordLen,
          "Record length %d is out of bound".format(recordLen))
        val buf = new Array[Byte](recordLen)
        openFile.dataStream.readFully(buf)
        val eor = openFile.dataStream.readInt()
        require(eor == EOR, "End of record magic number 0x%x not 0x%x".format(eor, EOR))
        require(crc == computeRecordCrc(buf), "CRC should be %d but is %d".format(crc, computeRecordCrc(buf)))
        val record = recordSerializer.deserialize(buf)
        FileLogRecord(record, position, openFile.file)
      } else {
        // Reached the end of the current log file, continue with the next available log file
        openFile.close()
        currentLogfile = Some(logFiles.next())
        next()
      }
    }

    def close() {
      currentLogfile.foreach(_.close())
      currentLogfile = None
    }

    /**
     * Skip to the last file interval.
     */
    def skipToCurrentLogFileLastInterval() {
      ensureInitialized()

      currentLogfile match {
        case Some(openFile) => {
          val fileSkipIntervalSize = openFile.header.fileSkipIntervalSize
          val fileLength = openFile.channel.size()
          val lastSkipPosition = fileLength - (fileLength % fileSkipIntervalSize)
          val skipLen = lastSkipPosition - openFile.channel.position()
          if (skipLen > 0) {
            info("Skipping {} bytes to last interval (intervalSize={}, size={}): {}",
              skipLen, fileSkipIntervalSize, fileLength, openFile.file)
            openFile.dataStream.skip(skipLen)
          }
        }
        case None => warn("Cannot skip to last interval. No current open log file")
      }
    }

    private def ensureInitialized() {
      if (!initialized) {
        currentLogfile = if (logFiles.hasNext) Some(logFiles.next()) else None
        initialized = true
      }
    }

    private def openLogFile(file: File): Option[OpenLogFile] = {
      readOpenMeter.mark()

      if (file.length() == 0) {
        info("Empty log file: {}", file)
        None
      } else {
        info("Opening log file: {}", file)
        val fileStream = new FileInputStream(file)
        val dataStream = new DataInputStream(fileStream)
        val header = readFileHeader(file, dataStream)
        if (fileStream.getChannel.position() < fileStream.getChannel.size) {
          Some(new OpenLogFile(file, header, dataStream, fileStream))
        } else None
      }
    }

    private def readFileHeader(file: File, dis: DataInputStream): FileHeader = {
      try {
        val readMagic = dis.readLong()
        val readVersion = dis.readInt()
        val readService = dis.readUTF()
        val readSkipIntervalSize = if (readVersion > 1) dis.readInt() else Int.MaxValue
        FileHeader(readMagic, readVersion, readService, readSkipIntervalSize)
      } catch {
        case e: Exception => {
          warn("Error reading log file header from {}: {}", file, e)
          throw e
        }
      }
    }
  }

}

/**
 * Iterator which returns the log files in ascending order that contain transactions from the specified initial index.
 * Includes log file added after the iterator creation.
 */
private[log] class LogFileIterator(txLog: FileTransactionLog, initialIndex: Index) extends Iterator[File] {
  private var logFiles = txLog.getLogFiles(initialIndex).toIterator
  private var lastFile: Option[File] = None

  def hasNext = {
    if (!logFiles.hasNext) {
      logFiles = lastFile match {
        case Some(file) => {
          val lastFileIndex = txLog.getIndexFromName(file.getName)
          txLog.getLogFiles(lastFileIndex).filter(Some(_) != lastFile).toIterator
        }
        case None => txLog.getLogFiles(initialIndex).toIterator
      }
    }
    logFiles.hasNext
  }

  def next() = {
    val file = logFiles.next()
    lastFile = Some(file)
    file
  }
}

object FileTransactionLog {
  val LogFileMagic: Long = ByteBuffer.wrap("NRVTXLOG".getBytes).getLong
  val LogFileCurVersion: Int = 3
  val LogFileMinVersion: Int = 3
  val LogFileMaxVersion: Int = 3

  val MinRecordLen = 0
  val MaxRecordLen = 2000000
  val EOR: Int = 0x5a
}
