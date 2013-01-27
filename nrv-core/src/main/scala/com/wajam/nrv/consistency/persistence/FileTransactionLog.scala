package com.wajam.nrv.consistency.persistence

import com.wajam.nrv.utils.timestamp.Timestamp
import java.nio.ByteBuffer
import java.io._
import java.util.zip.CRC32
import com.wajam.nrv.Logging
import com.yammer.metrics.scala.Instrumented
import scala.Some

/**
 * Class for writing and reading transaction logs
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
 * checksum TxnEvent 0x5A
 *
 * checksum: 8bytes CRC32 calculated across TxnEvent and 0x5A
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
class FileTransactionLog(service: String, token: Long, val logDir: String,
                         serializer: TransactionEventSerializer = new TransactionEventSerializer)
  extends Logging with Instrumented {

  import FileTransactionLog._

  private val syncTimer = metrics.timer("sync-time")

  private val filePrefix = "%s-%010d".format(service, token)

  private var fileStreams: List[FileOutputStream] = List()
  private var logStream: Option[DataOutputStream] = None

  private var lastTimestamp: Option[Timestamp] = getLastLoggedTimestamp

  /**
   * Returns the most recent timestamp written on disk. This method scan the log files to find the timestamp
   */
  def getLastLoggedTimestamp: Option[Timestamp] = {
    // Iterate to the last transaction of the last log file
    getLogFilesFrom(Timestamp(0)).lastOption match {
      case Some(file) => {
        read(getTimestampFromName(file.getName)).toIterable.lastOption match {
          case Some(tx) => Some(tx.timestamp)
          case _ => None
        }
      }
      case _ => None
    }
  }

  /**
   * Appends the specified transaction event to the transaction log
   */
  def append(tx: TransactionEvent) {

    // Validate previous timestamp match last transaction event timestamp
    if (tx.previous != lastTimestamp) {
      throw new IOException("This transaction %s previous timestamp %s is not %s".format(
        tx, tx.previous, lastTimestamp))
    }

    // Validate transaction timestamp is newer than previous timestamp
    tx.previous match {
      case Some(previousTimestamp) if previousTimestamp >= tx.timestamp => {
        throw new IOException("This transaction %s previous timestamp %s >= %s".format(
          tx, previousTimestamp, tx.timestamp))
      }
      case _ =>
    }

    logStream match {
      case None => {
        // No log file open, create a new log file
        val logFileName = getNameFromTimestamp(tx.timestamp)
        info("Creating new log file: {}", logFileName)

        val fos = new FileOutputStream(new File(logDir, logFileName))
        val dos = new DataOutputStream(new BufferedOutputStream(fos))
        writeFileHeader(dos)

        logStream = Some(dos)
        fileStreams = fos :: fileStreams

        // Finally write the transaction event to the newly created log file
        writeTransactionEvent(dos, tx)
      }
      case Some(dos) => {
        // Write the transaction event into th currently open log file
        writeTransactionEvent(dos, tx)
      }
    }

    lastTimestamp = Some(tx.timestamp)
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
    new TransactionLogIterator(timestamp)
  }

  /**
   * Truncate log from the specified timestamp inclusively
   */
  def truncate(timestamp: Timestamp): Boolean = {
    // TODO: to implement
    false
  }

  /**
   * Rollover the current log file
   */
  def rollLog() {
    logStream.foreach(_.flush())
    logStream = None
  }

  /**
   * Ensure that transaction log is fully written on disk
   */
  def commit() {
    logStream.foreach(_.flush())

    for (fos <- fileStreams) {
      fos.flush()
      syncTimer.time {
        fos.getChannel.force(false)
      }
    }

    val toClose = fileStreams.tail
    fileStreams = List(fileStreams.head)
    toClose.foreach(_.close())
  }

  /**
   * Close this transaction log
   */
  def close() {
    logStream.foreach(_.close())
    fileStreams.foreach(_.close())
  }

  /**
   * Returns all the log files containing transactions from the specified timestamp. The files are returned in
   * timestamp ascending order.
   */
  private[persistence] def getLogFilesFrom(timestamp: Timestamp): Iterable[File] = {
    val logFiles = new File(logDir).listFiles(new FilenameFilter {
      def accept(dir: File, name: String) = {
        name.startsWith(filePrefix) && name.endsWith(".log")
      }
    })

    if (logFiles.isEmpty) {
      logFiles
    } else {
      val sortedFiles = logFiles.sortWith((f1, f2) => getTimestampFromName(f1.getName) < getTimestampFromName(f2.getName))
      sortedFiles.indexWhere(f => getTimestampFromName(f.getName) > timestamp) match {
        case -1 => List(sortedFiles.last)
        case 0 => sortedFiles
        case i => sortedFiles.drop(i - 1)
      }
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

  class TransactionLogIterator(timestamp: Timestamp) extends Iterator[TransactionEvent] {
    private var logStream: Option[DataInputStream] = None
    private var nextTx: Option[TransactionEvent] = None

    // Initialize iterator to the initial transaction
    private val logFiles = getLogFilesFrom(timestamp).toIterator
    openNextFile()
    while (nextTx.isDefined && nextTx.get.timestamp < timestamp) {
      readNextTransaction()
    }

    def hasNext = nextTx.isDefined

    def next(): TransactionEvent = {
      val result = nextTx.get

      // Try to advance to the future next transaction
      readNextTransaction()

      result
    }

    def close() {
      logStream.foreach(_.close())
      logStream = None
    }

    private def openNextFile(): Boolean = {
      logStream.foreach(_.close())
      logStream = None

      if (logFiles.hasNext) {
        val file = logFiles.next()
        if (file.length() == 0) {
          info("Skipping empty log file: {}", file)
          openNextFile()
        } else {
          info("Opening log file: {}", file)
          val dis = new DataInputStream(new FileInputStream(file))
          readFileHeader(dis)
          logStream = Some(dis)
          readNextTransaction()
        }
      } else {
        false
      }
    }

    private def readFileHeader(dis: DataInputStream) {
      // TODO: header validation
      dis.readLong() // LogFileMagic
      dis.readInt() // LogFileVersion
      dis.readUTF() // Service
    }

    private def readNextTransaction(): Boolean = {
      logStream match {
        case Some(dis) => {
          try {
            val crc = dis.readLong() // TODO: crc validation
            val txLen = dis.readInt() // TODO: protection against negative or len too large
            val buf = new Array[Byte](txLen)
            dis.read(buf) // TODO: validate read len
            val tx = serializer.deserialize(buf)
            val eot = dis.readInt() // TODO: validate eot
            nextTx = Some(tx)
            true
          } catch {
            case e: EOFException => {
              nextTx = None
              openNextFile()
            }
          }
        }
        case _ => false
      }
    }
  }
}

object FileTransactionLog {
  val LogFileMagic: Long = ByteBuffer.wrap("NRVTXLOG".getBytes).getLong
  val LogFileVersion: Int = 0
  val EOT: Int = 0x5a
}
