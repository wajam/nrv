package com.wajam.nrv.consistency.persistence

import com.wajam.nrv.utils.timestamp.Timestamp
import java.nio.ByteBuffer
import java.io._
import java.util.zip.CRC32
import com.wajam.nrv.Logging
import com.yammer.metrics.scala.Instrumented

/**
 * Class for writing and reading transaction logs.
 * IMPORTANT: This class is not thread safe and must be invoked from a single thread.
 *
 * <p>
 * The format of a Transactional log is as follows:
 * <blockquote><pre>
 * LogFile:
 * FileHeader TxnList
 *
 * FileHeader: {
 * magic 8bytes "NRVTXLOG"
 * version 4bytes
 * servicename mutf-8
 * }
 *
 * TxnList:
 * Txn || Txn TxnList
 *
 * Txn:
 * checksum TxnLen TxnEvent 0x5A
 *
 * checksum: 8bytes CRC32 calculated across TxnLen, TxnEvent and 0x5A
 *
 * TxnEvent: {
 * timestamp 8bytes
 * lasttimestamp 8bytes (???)
 * token 8bytes
 * msglen 4bytes
 * message
 * }
 *
 * </pre></blockquote>
 */
class FileTransactionLog(val service: String, val token: Long, val logDir: String,
                         serializer: TransactionEventSerializer = new TransactionEventSerializer,
                         validateTimestamp: Boolean = true)
  extends Logging with Instrumented {

  import FileTransactionLog._

  lazy private val appendTimer = metrics.timer("append-time")
  lazy private val appendMeter = metrics.meter("append-calls", "append-calls")
  lazy private val truncateTimer = metrics.timer("truncate-time")
  lazy private val truncateMeter = metrics.meter("truncate-calls", "truncate-calls")
  lazy private val commitMeter = metrics.meter("commit-calls", "commit-calls")
  lazy private val commitSyncTimer = metrics.timer("commit-sync-time")
  lazy private val rollMeter = metrics.meter("roll-calls", "roll-calls")

  lazy private val readMeter = metrics.meter("read-calls", "read-calls")
  lazy private val readInitTimer = metrics.timer("read-init-time")
  lazy private val readNextCalls = metrics.meter("read-next-calls", "read-next-calls")
  lazy private val readNextError = metrics.meter("read-next-error", "read-next-error")
  lazy private val readNextTimer = metrics.timer("read-next-time")
  lazy private val readOpenMeter = metrics.meter("read-open-calls", "read-open-calls")

  // TODO: Add write-bytes and read-bytes meters

  private val filePrefix = "%s-%010d".format(service, token)

  private var fileStreams: List[FileOutputStream] = List()
  private var logStream: Option[DataOutputStream] = None
  private var writeFile: File = null

  private var lastTimestamp: Option[Timestamp] = getLastLoggedTimestamp

  override def toString = {
    "service=%s, tk=%d, last-ts=%s, open=%s".format(service, token, lastTimestamp.getOrElse(""), writeFile)
  }

  /**
   * Returns the most recent timestamp written on disk. This method scan the log files to find the last timestamp
   */
  def getLastLoggedTimestamp: Option[Timestamp] = {
    // Read the last timestamp from the last file. If the file is empty, do the same same the previous one and so on
    // until a timestamp is found.
    var result: Option[Timestamp] = None
    getLogFilesFrom(Timestamp(0)).toList.reverse.find(file => {
      read(getTimestampFromName(file.getName)).toIterable.lastOption match {
        case Some(tx) => {
          result = Some(tx.timestamp)
          true
        }
        case _ => false
      }
    })

    result
  }

  /**
   * Appends the specified transaction event to the transaction log
   */
  def append(tx: TransactionEvent) {
    appendMeter.mark()

    appendTimer.time {
      // Ensure previous timestamp match last transaction event timestamp
      if (validateTimestamp) {
        require(tx.previous == lastTimestamp, "This transaction %s previous timestamp %s is not %s".format(
          tx, tx.previous, lastTimestamp))

        // Ensure transaction timestamp is newer than previous timestamp
        require(tx.previous.forall(_ < tx.timestamp), "This transaction %s previous timestamp %s >= %s".format(
          tx, tx.previous.get, tx.timestamp))
      }

      // Write the transaction event into the open log file
      writeTransactionEvent(logStream.getOrElse(openNewLogFile(tx.timestamp)), tx)
      lastTimestamp = Some(tx.timestamp)
    }
  }

  private def openNewLogFile(timestamp: Timestamp) = {
    val file = new File(logDir, getNameFromTimestamp(timestamp))
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

  private def writeTransactionEvent(dos: DataOutputStream, tx: TransactionEvent) {
    val crc = new CRC32()
    val buf = serializer.serialize(tx)
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
   * Read all the transaction from the specified timestamp
   */
  def read(timestamp: Timestamp): TransactionLogIterator = {
    readMeter.mark()

    readInitTimer.time {
      new TransactionLogIterator(timestamp)
    }
  }

  /**
   * Truncate log from the specified timestamp inclusively
   */
  def truncate(timestamp: Timestamp): Boolean = {
    // TODO: to implement
    truncateMeter.mark()

    truncateTimer.time {
      false
    }
  }

  /**
   * Rollover the current log file
   */
  def rollLog() {
    rollMeter.mark()

    logStream.foreach(_.flush())
    logStream = None
    writeFile = null
  }

  /**
   * Ensure that transaction log is fully written on disk
   */
  def commit() {
    commitMeter.mark()

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

  /**
   * Close this transaction log
   */
  def close() {
    logStream.foreach(_.close())
    fileStreams.foreach(_.close())
    writeFile = null
  }

  /**
   * Returns all the log files containing transactions from the specified timestamp. The files are returned in
   * timestamp ascending order.
   */
   def getLogFilesFrom(timestamp: Timestamp): Iterable[File] = {
    val logFiles = new File(logDir).listFiles(new FilenameFilter {
      def accept(dir: File, name: String) = {
        name.startsWith(filePrefix) && name.endsWith(".log")
      }
    })

    val sortedFiles = logFiles.sortWith((f1, f2) => getTimestampFromName(f1.getName) < getTimestampFromName(f2.getName))
    sortedFiles.indexWhere(f => getTimestampFromName(f.getName) > timestamp) match {
      case -1 if sortedFiles.isEmpty => sortedFiles
      case -1 => List(sortedFiles.last)
      case 0 => sortedFiles
      case i => sortedFiles.drop(i - 1)
    }
  }

  private[persistence] def getNameFromTimestamp(timestamp: Timestamp): String = {
    "%s-%d.log".format(filePrefix, timestamp.value)
  }

  private[persistence] def getTimestampFromName(name: String): Timestamp = {
    val end = name.lastIndexOf(".")
    val start = name.lastIndexOf("-", end) + 1
    Timestamp(name.substring(start, end).toLong)
  }

  class TransactionLogIterator(initialTimestamp: Timestamp) extends Iterator[TransactionEvent] {
    private var logStream: Option[DataInputStream] = None
    private var nextTx: Either[Exception, Option[TransactionEvent]] = Right(None)
    private var readFile: File = null

    // Position the iterator to the initial timestamp by reading tx from log as long the read timestamp is before the
    // initial timestamp
    private val logFiles = getLogFilesFrom(initialTimestamp).toIterator
    openNextFile()
    while (nextTx match {
      case Right(Some(tx)) => tx.timestamp < initialTimestamp
      case Left(e) => throw new IOException(e)
      case _ => false
    }) {
      readNextTransaction()
    }

    def hasNext: Boolean = {
      nextTx match {
        case Right(Some(tx)) => true
        case _ => false
      }
    }

    def next(): TransactionEvent = {
      readNextCalls.mark()

      val result = nextTx match {
        case Right(Some(tx)) => tx
        case Left(e) => throw new IOException(e)
        case _ => throw new IllegalStateException("Must not be invoked beyond the end of the iterator")
      }

      // Read ahead the next transaction for a future call
      readNextTransaction()

      result
    }

    def close() {
      logStream.foreach(_.close())
      logStream = None
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
          val dis = new DataInputStream(new FileInputStream(readFile))
          if (readFileHeader(dis)) {
            logStream = Some(dis)
            readNextTransaction()
          } else {
            nextTx = Right(None)
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
          nextTx = Left(e)
          false
        }
      }
    }

    private def readNextTransaction(): Boolean = {
      readNextTimer.time {
        logStream match {
          case Some(dis) => {
            try {
              val crc = dis.readLong() // TODO: crc validation
              try {
                val txLen = dis.readInt() // TODO: protection against negative or len too large
                val buf = new Array[Byte](txLen)
                dis.read(buf) // TODO: validate read len
                val tx = serializer.deserialize(buf)
                val eot = dis.readInt() // TODO: validate eot
                nextTx = Right(Some(tx))
                true
              } catch {
                case e: Exception => {
                  readNextError.mark()
                  warn("Error reading transaction from {}: {}", readFile, e)
                  nextTx = Left(e)
                  false
                }
              }
            } catch {
              case e: EOFException => {
                info("End of log file: {}", readFile)
                nextTx = Right(None)
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
  val LogFileVersion: Int = 0
  val EOT: Int = 0x5a
}
