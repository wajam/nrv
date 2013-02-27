package com.wajam.nrv.consistency

import persistence.LogRecord._
import persistence._
import com.wajam.nrv.utils.timestamp.Timestamp
import collection.immutable.TreeMap
import java.io.File
import com.wajam.nrv.utils.IdGenerator
import java.nio.file.Files
import scala.Some
import com.wajam.nrv.Logging

class TransactionRecovery(logDir: String, store: ConsistentStore, idGenerator: IdGenerator[Long]) extends Logging {

  val recoveryDir = new File(logDir, "recovery")

  def recoverMember(member: ResolvedServiceMember) {

    // Complete and/or cleanup any previous recovery attempts
    resumeMemberRecovery(member)

    val memberTxLog = new FileTransactionLog(member.serviceName, member.token, logDir)
    val lastLoggedRecord = memberTxLog.getLastLoggedRecord
    lastLoggedRecord match {
      case Some(Index(_, Some(consistentTimestamp))) => {
        // The last record is an Index record so we know that the member log is consistent up to that point.
        // Truncate database after last consistent timestamp for the member token ranges to ensure the store
        // is consistent.
        // TODO: Is this NEEDED???? If yes, we need to do it at the end of the recovery.
//        store.truncateFrom(Timestamp(consistentTimestamp.value + 1), member.ranges)
      }
      case Some(lastRecord) => {
        // Load all records from the last consistent timestamp and truncate the incomplete transactions in the store
        val pending = loadAllPending(memberTxLog, lastRecord.consistentTimestamp)
        val incomplete = pending.values.filter(_.response.isEmpty)
        incomplete.foreach(tx => store.truncateAt(tx.timestamp, tx.token))

        // Rewrite complete transactions records in recovery log files
        recoveryDir.mkdir()
        val recoveryTxLog = new FileTransactionLog(member.serviceName, member.token, recoveryDir.getAbsolutePath)
        val complete = pending.values.filter(_.response.isDefined)
        var lastConsistentTimestamp = lastRecord.consistentTimestamp
        for (tx <- complete; response <- tx.response) {
          recoveryTxLog.append(Request(idGenerator.nextId, lastConsistentTimestamp, tx.timestamp,
            tx.token, tx.request.message))
          recoveryTxLog.append(Response(idGenerator.nextId, lastConsistentTimestamp, response.timestamp,
            response.token, response.status))
          lastConsistentTimestamp = Some(tx.timestamp)
        }
        recoveryTxLog.append(Index(idGenerator.nextId, lastConsistentTimestamp))

        // Replace recovered records with the rewritten records
        finalizeRecovery(memberTxLog, recoveryTxLog)
      }
      case None => // Log is empty, nothing to recover
    }
  }

  case class PendingTransaction(request: Request, var response: Option[Response] = None) {
    def timestamp = request.timestamp

    def token = request.token
  }

  /**
   * Verify if a previous recovery procedure is incomplete and try to complete it or if this is not possible try to
   * clean up the previous recovery attempt to start the recovery again.
   */
  private def resumeMemberRecovery(member: ResolvedServiceMember) {
    if (recoveryDir.exists()) {
      val recoveryTxLog = new FileTransactionLog(member.serviceName, member.token, recoveryDir.getAbsolutePath)
      if (recoveryTxLog.getLogFiles.size > 0) {
        info("") // TODO:
        val memberTxLog = new FileTransactionLog(member.serviceName, member.token, logDir)
        val lastLoggedRecord = memberTxLog.getLastLoggedRecord
        (lastLoggedRecord, recoveryTxLog.getLastLoggedRecord) match {
          case (Some(lastRecord), Some(lastRecoveredIndex: Index)) => {
            if (lastRecoveredIndex.consistentTimestamp.getOrElse(Timestamp(Long.MinValue)) >
              lastRecord.consistentTimestamp.getOrElse(Timestamp(Long.MinValue))) {
              // The recovery log is more up to date than the regular log files. The
              // previous recovery attempt failed after the regular log file have been truncated.
              // Finalize the recovery by moving the rewritten recovery log to the regular log directory.
              // TODO: warning + finalize

              finalizeRecovery(memberTxLog, recoveryTxLog)
            } else {
              // Recovery log is not more up to date, something very fishy has happened during the previous recovery.
              // Resolution must be handled manually.
              // TODO: fail recovery
            }
          }
          case (None, Some(lastRecoveredIndex)) => {
            // No regular log, just recovery ones! What is going on here???
            // TODO: fail recovery
          }
          case _ => {
            // No pending recovery log or previous recovery is incomplete (i.e. last recovery record is not Index),
            // Delete any pending recovery files to have a clean recovery environment.
            // TODO: warning
            for (recoveryFile <- recoveryTxLog.getLogFiles.toList.reverse) {
              Files.delete(recoveryFile.toPath)
            }
          }
        }
      }
    }
  }

  private def finalizeRecovery(memberTxLog: FileTransactionLog, recoveryTxLog: FileTransactionLog) {
    // Truncate member log from the first recovered record consistent timestamp
    val firstRecoveredRecord = firstRecord(recoveryTxLog).flatMap(r => firstRecord(memberTxLog, r.consistentTimestamp))
    firstRecoveredRecord.foreach(memberTxLog.truncate(_))

    // Move recovery log files into the real log directory
    for (recoveryFile <- recoveryTxLog.getLogFiles) {
      Files.move(recoveryFile.toPath, new File(logDir, recoveryFile.getName).toPath)
    }

    // Delete recovery directory
    recoveryDir.delete() // TODO: really???
  }

  /**
   * Load all transactions (complete and incomplete) starting at the specified consistent timestamp
   */
  private def loadAllPending(txLog: FileTransactionLog, consistentTimestamp: Option[Timestamp]): TreeMap[Timestamp, PendingTransaction] = {
    val it = consistentTimestamp match {
      case Some(timestamp) => txLog.read(timestamp)
      case None => txLog.read
    }
    try {
      var pending: TreeMap[Timestamp, PendingTransaction] = TreeMap()
      for (record <- it) {
        info(record.toString)
        record match {
          case request: Request => pending += (request.timestamp -> PendingTransaction(request))
          case response: Response => pending(response.timestamp).response = Some(response)
          case _ => // Ignore index records
        }
      }
      pending
    } finally {
      it.close()
    }
  }

  /**
   * Returns the first record starting at the specified consistent timestamp
   */
  private def firstRecord(txLog: FileTransactionLog, consistentTimestamp: Option[Timestamp] = None): Option[LogRecord] = {
    val it = consistentTimestamp match {
      case Some(timestamp) => txLog.read(timestamp)
      case None => txLog.read
    }
    try {
      if (it.hasNext) Some(it.next()) else None
    } finally {
      it.close()
    }
  }
}
