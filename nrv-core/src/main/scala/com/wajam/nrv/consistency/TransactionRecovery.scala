package com.wajam.nrv.consistency

import persistence.LogRecord._
import persistence._
import com.wajam.nrv.utils.timestamp.Timestamp
import java.io.File
import com.wajam.nrv.utils.IdGenerator
import java.nio.file.Files
import scala.Some
import com.wajam.nrv.Logging
import collection.mutable
import com.yammer.metrics.scala.Instrumented

/**
 * Helper class that ensure that service members transaction log are consistent with the consistent store.
 * This class is not thread safe and should not be called from mutiple threads.
 */
class TransactionRecovery(logDir: String, store: ConsistentStore, idGenerator: IdGenerator[Long])
  extends Logging with Instrumented {

  lazy private val resumeTimer = metrics.timer("resume-time")
  lazy private val resumeFinalize = metrics.meter("resume-finalize", "resume-finalize")
  lazy private val resumeError = metrics.meter("resume-error", "resume-error")
  lazy private val resumeClean = metrics.meter("resume-clean", "resume-clean")

  lazy private val recoverTimer = metrics.timer("recover-time")
  lazy private val recoverTruncateTimer = metrics.timer("recover-truncate-record")
  lazy private val recoverRewrite = metrics.meter("recover-rewrite-record", "recover-rewrite-record")

  val recoveryDir = new File(logDir, "recovery")

  /**
   * Ensure that the specified service member transaction log is consistent.
   * <p><p>
   * The recovery process read the the transaction log from the last known consistent timestamp. The incomplete
   * transactions are trucated from the storage and the member log. The complete transactions are rewriten in the log
   * and marked as consistent.
   * <p><p>
   * The recovery process can resume from a previously incomple recovery attempt (i.e. jvm kill during recovery).
   */
  def recoverMember(member: ResolvedServiceMember) {

    // Complete and/or cleanup any previously incomplete recovery attempt
    resumeMemberRecovery(member)

    recoverTimer.time {
      val memberTxLog = new FileTransactionLog(member.serviceName, member.token, logDir)
      val lastLoggedRecord = memberTxLog.getLastLoggedRecord
      lastLoggedRecord match {
        case Some(Index(_, Some(consistentTimestamp))) => {
          // The last record is an Index record so we know that the member log is consistent up to that point.
          // Truncate database after last consistent timestamp for the member token ranges to ensure the store
          // is consistent.
          info("Transaction log already consistent. Skipping transaction log recovery for member {}.", member)
          // TODO: Truncate db from last consistent timestamp???? If yes, we need to do it at the end of the recovery.
//        store.truncateFrom(Timestamp(consistentTimestamp.value + 1), member.ranges)
        }
        case Some(lastRecord) => {
          // Load all records from the last consistent timestamp and truncate the incomplete transactions in the store
          val pending = loadAllPending(memberTxLog, lastRecord.consistentTimestamp)
          val incomplete = pending.filter(_.response.isEmpty)
          info("Truncating {} incomplete records for member {}.", incomplete.size, member)
          incomplete.foreach(tx => recoverTruncateTimer.time(store.truncateAt(tx.timestamp, tx.token)))

          // Rewrite complete transactions records in separate recovery log files
          recoveryDir.mkdir()
          val recoveryTxLog = new FileTransactionLog(member.serviceName, member.token, recoveryDir.getAbsolutePath)
          val complete = pending.filter(_.response.isDefined)
          info("Rewriting {} complete records for member {}.", complete.size, member)
          // Find consistent timestamp of the first transaction to rewrite
          var lastConsistentTimestamp = for {
            firstCompleteTx <- complete.headOption
            firstCompleteResponse <- firstCompleteTx.response
            firstCompleteConsistentTimestamp <- firstCompleteResponse.consistentTimestamp
          } yield firstCompleteConsistentTimestamp
          for (tx <- complete; response <- tx.response) {
            tx.request.foreach(request => {
              // Rewrite request only if available. Not available if request record have originally been written before
              // the last consistent timestamp and the response record after. We are only processing the records written
              // after the last known consistent timestamp.
              recoveryTxLog.append(Request(idGenerator.nextId, lastConsistentTimestamp, request.timestamp,
                request.token, request.message))
              recoverRewrite.mark()
            })
            recoveryTxLog.append(Response(idGenerator.nextId, lastConsistentTimestamp, response.timestamp,
              response.token, response.status))
            recoverRewrite.mark()
            lastConsistentTimestamp = Some(tx.timestamp)
          }
          // Append an index record at the end of the recovery log. This stamp approve that all the rewritten records
          // are consistent.
          recoveryTxLog.append(Index(idGenerator.nextId, lastConsistentTimestamp))

          // Replace recovered records with the rewritten records
          finalizeRecovery(memberTxLog, recoveryTxLog)
        }
        case None => // Log is empty, nothing to recover
      }
    }
  }

  private case class PendingTransaction(request: Option[Request] = None, var response: Option[Response] = None)
    extends Ordered[PendingTransaction] {
    require(request.isDefined || response.isDefined, "Request or response must be defined")

    val timestamp = request.getOrElse(response.get).timestamp

    val token = request.getOrElse(response.get).token

    def compare(that: PendingTransaction) = timestamp.compare(that.timestamp)
  }

  /**
   * Verify if a previous recovery procedure is incomplete and try to complete it or if this is not possible try to
   * clean up the previous recovery attempt to start the recovery again.
   */
  private def resumeMemberRecovery(member: ResolvedServiceMember) {
    if (recoveryDir.exists()) {
      val recoveryTxLog = new FileTransactionLog(member.serviceName, member.token, recoveryDir.getAbsolutePath)
      val recoveryFiles = recoveryTxLog.getLogFiles
      if (!recoveryFiles.isEmpty) {
        info("Recovery files found and resuming recovery if possible ({}).",
          recoveryFiles.map(_.getName).mkString(","))
        resumeTimer.time {
          // Compare the last records from the member log and previous recovery log
          val memberTxLog = new FileTransactionLog(member.serviceName, member.token, logDir)
          (memberTxLog.getLastLoggedRecord, recoveryTxLog.getLastLoggedRecord) match {
            case (Some(lastRecord), Some(lastRecoveredIndex: Index)) => {
              if (lastRecoveredIndex.consistentTimestamp.getOrElse(Timestamp(Long.MinValue)) >
                lastRecord.consistentTimestamp.getOrElse(Timestamp(Long.MinValue))) {
                // The recovery log is more up to date than the regular log files. The
                // previous recovery attempt failed after the regular log file have been truncated.
                // Finalize the recovery by moving the rewritten recovery log to the regular log directory.
                warn("Previous recovery wasn't complete but can be finalized. " +
                  "Finalizing recovery log for member {}.", member)
                resumeFinalize.mark()
                finalizeRecovery(memberTxLog, recoveryTxLog)
              } else {
                // Member log is more up to date than pending recovery log. Something very fishy, like someone messing
                // with the logs (don't do that AP!!!), has happened during or after the previous recovery attempt.
                // Resolution must be handled manually.
                resumeError.mark()
                error("Previous recovery wasn't complete and the pending recovery log are outdated. " +
                  "Cannot recover member {}.", member)
                // TODO: fail recovery
                throw new ConsistencyException()
              }
            }
            case (None, Some(lastRecoveredIndex)) => {
              // No member log, just recovery ones! Someone deleted the member log during or after the previous attemp!!!
              resumeError.mark()
              error("Previous recovery wasn't complete. There are no member transaction logs, just the partially " +
                "recovered ones. Cannot recover member {}.", member)
              // TODO: fail recovery
              throw new ConsistencyException()
            }
            case _ => {
              // No pending recovery log or previous recovery is incomplete (i.e. last recovery record is not Index),
              // Delete any pending recovery files to have a clean recovery environment.
              warn("Previous recovery wasn't complete and cannot be finalized. " +
                "Cleaning up before restarting recovery again for member {}.", member)
              resumeClean.mark()
              for (recoveryFile <- recoveryTxLog.getLogFiles.toList.reverse) {
                Files.delete(recoveryFile.toPath)
              }
            }
          }
        }
      }
    }
  }

  private def finalizeRecovery(memberTxLog: FileTransactionLog, recoveryTxLog: FileTransactionLog) {
    // Truncate member log from the first recovered record consistent timestamp
    val firstRecoveredRecord = firstRecord(recoveryTxLog).flatMap(r => firstRecord(memberTxLog, Some(r.timestamp)))
    firstRecoveredRecord.foreach(memberTxLog.truncate(_))

    // Move recovery log files into the real log directory
    for (recoveryFile <- recoveryTxLog.getLogFiles) {
      Files.move(recoveryFile.toPath, new File(logDir, recoveryFile.getName).toPath)
    }

    // Delete recovery directory (will fail if not empty but this is ok)
    recoveryDir.delete()
  }

  /**
   * Returns all transactions (complete and incomplete) starting at the specified consistent timestamp
   * and ordered by timestamp.
   */
  private def loadAllPending(txLog: FileTransactionLog, consistentTimestamp: Option[Timestamp]): Seq[PendingTransaction] = {
    val it = consistentTimestamp match {
      case Some(timestamp) => txLog.read(timestamp)
      case None => txLog.read
    }
    try {
      val pending: mutable.Map[Timestamp, PendingTransaction] = mutable.Map()
      for (record <- it) {
        record match {
          case request: Request => {
            pending += (request.timestamp -> PendingTransaction(Some(request)))
          }
          case response: Response => {
            pending.getOrElseUpdate(response.timestamp,
              PendingTransaction(response = Some(response))).response = Some(response)
          }
          case _ => // Ignore index records
        }
      }
      pending.values.toIndexedSeq.sorted
    } finally {
      it.close()
    }
  }

  /**
   * Returns the first record starting at the specified consistent timestamp.
   */
  private def firstRecord(txLog: FileTransactionLog, consistentTimestamp: Option[Timestamp] = None): Option[TimestampedRecord] = {
    val it = consistentTimestamp match {
      case Some(timestamp) => txLog.read(timestamp)
      case None => txLog.read
    }
    try {
      it.find(_ match {
        case record: TimestampedRecord => true
        case _ => false
      }).map(_.asInstanceOf[TimestampedRecord])
    } finally {
      it.close()
    }
  }
}
