package com.wajam.nrv.consistency

import com.wajam.nrv.consistency.log.LogRecord._
import com.wajam.nrv.consistency.log._
import java.io.File
import java.nio.file.Files
import com.wajam.nrv.Logging
import collection.mutable
import com.yammer.metrics.scala.Instrumented
import com.wajam.commons.{IdGenerator}
import com.wajam.nrv.utils.TimestampIdGenerator
import com.wajam.nrv.utils.timestamp.Timestamp

/**
 * Helper class that ensure that service members transaction log are consistent with the consistent store.
 */
class ConsistencyRecovery(logDir: String, store: ConsistentStore, serializer: Option[LogRecordSerializer] = None,
                          idGenerator: IdGenerator[Long] = new TimestampIdGenerator)
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
   * transactions are truncated from the storage and the service member log. The complete transactions are rewriten
   * in the log and marked as consistent.
   * <p><p>
   * The recovery process can resume from a previously incomplete recovery attempt (i.e. jvm kill during recovery).
   *
   * @param member the service member to restore
   * @return the final consistent index record
   */
  def restoreMemberConsistency(member: ResolvedServiceMember, onRecoveryFailure: => Unit): Option[Index] = {

    // Complete and/or cleanup any previously incomplete recovery attempt
    resumeMemberConsistencyRecovery(member, onRecoveryFailure)

    recoverTimer.time {
      val memberTxLog = new FileTransactionLog(member.serviceName, member.token, logDir, serializer = serializer)
      val lastLoggedRecord = memberTxLog.getLastLoggedRecord
      lastLoggedRecord match {
        case Some(lastRecord: TimestampedRecord) => {
          // The last record is NOT an Index record so we need to perform recovery.
          rewriteFromRecord(lastRecord, memberTxLog, member)
        }
        case Some(index: Index) => {
          // Log is terminated by an Index, nothing to recover
          Some(index)
        }
        case None => None
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

  def rewriteFromRecord(pivotRecord: TimestampedRecord, memberTxLog: FileTransactionLog,
                        member: ResolvedServiceMember): Option[Index] = {
    // Load all records from the pivot record consistent timestamp and truncate the transactions without a response
    // in the store
    val (consistentTxOpt, pending) = loadAllPending(memberTxLog, pivotRecord.consistentTimestamp)
    val (noResponse, withResponse) = pending.partition(_.response.isEmpty)
    info("Truncating {} incomplete records for member {}.", noResponse.size, member)
    noResponse.foreach(tx => recoverTruncateTimer.time(store.truncateAt(tx.timestamp, tx.token)))

    // Rewrite transaction records with a response in a separate recovery log files. Rewrite first the last
    // consistent transaction, then the transactions with only responses (their request records exist before
    // the rewrite position), and then finaly the remaining complete transactions. Both the response only
    // sequence and the complete transaction sequence are ordered by timestamps in their respective collection.
    recoveryDir.mkdir()
    val recoveryTxLog = new FileTransactionLog(member.serviceName, member.token, recoveryDir.getAbsolutePath,
      serializer = serializer)
    val (complete, responseOnly) = withResponse.partition(_.request.isDefined)

    // Initialize the consistent timestamp of the first transaction to rewrite which is the consistent timestamp
    // of the consistent transaction (consistent transaction is the rewrite position in the original log). Also
    // initialize the minimum value of the next consistent timestamp which is the timestamp of the consistent
    // transaction.
    var (minimumConsistentTimestamp, currentConsistentTimestamp) = consistentTxOpt match {
      case Some(PendingTransaction(Some(request), _)) => (Some(request.timestamp), request.consistentTimestamp)
      case None => (None, None)
      case _ => {
        throw new IllegalStateException("Consistent transaction MUST have a request (member=%s)".format(member))
      }
    }

    // Rewrite the consistent transaction
    info("Rewriting consistent transaction for member {}.", member)
    for (tx <- consistentTxOpt; response <- tx.response) {
      rewriteTransaction(tx, currentConsistentTimestamp, recoveryTxLog)
      recoverRewrite.mark()
    }

    // Rewrite reponses only
    info("Rewriting {} response records for member {}.", responseOnly.size, member)
    for (tx <- responseOnly; response <- tx.response) {
      rewriteTransaction(tx, currentConsistentTimestamp, recoveryTxLog)
      recoverRewrite.mark()
    }

    // Rewrite complete transactions. Update the consistent timestamp after each complete transaction rewritten
    info("Rewriting {} complete records for member {}.", complete.size, member)
    for (tx <- complete; response <- tx.response) {
      rewriteTransaction(tx, currentConsistentTimestamp, recoveryTxLog)
      recoverRewrite.mark()

      // Update the current consistent timestamp. Must not be lesser than the minmum consistent timestamp
      currentConsistentTimestamp = minimumConsistentTimestamp match {
        case Some(minimum) if tx.timestamp > minimum => Some(tx.timestamp)
        case None => Some(tx.timestamp)
        case _ => currentConsistentTimestamp
      }
    }

    // Append an index record at the end of the recovery log with the final consistent timestamp.
    // This stamp approve that all the rewritten records are consistent.
    val finalConsistentTimestamp = List(currentConsistentTimestamp, minimumConsistentTimestamp).flatten match {
      case Nil => None
      case xs => Some(xs.max)
    }
    val index = Index(idGenerator.nextId, finalConsistentTimestamp)
    recoveryTxLog.append(index)

    // Replace recovered records with the rewritten records
    finalizeRecovery(memberTxLog, recoveryTxLog)

    Some(index)
  }

  /**
   * Rewrite the specified transaction
   */
  def rewriteTransaction(tx: PendingTransaction, consistentTimestamp: Option[Timestamp], txLog: FileTransactionLog) {
    tx.request match {
      case Some(request) => {
        txLog.append(Request(idGenerator.nextId, consistentTimestamp, request.timestamp,
          request.token, request.message))
      }
      case None => // No transaction to write
    }

    // Response is not optional
    val response = tx.response.get
    txLog.append(Response(idGenerator.nextId, consistentTimestamp, response.timestamp,
      response.token, response.status))
  }

  /**
   * Verify if a previous recovery procedure is incomplete and try to complete it or if this is not possible try to
   * clean up the previous recovery attempt to start the recovery again.
   */
  private def resumeMemberConsistencyRecovery(member: ResolvedServiceMember, onRecoveryFailure: => Unit) {
    if (recoveryDir.exists()) {
      val recoveryTxLog = new FileTransactionLog(member.serviceName, member.token, recoveryDir.getAbsolutePath,
        serializer = serializer)
      val recoveryFiles = recoveryTxLog.getLogFiles
      if (!recoveryFiles.isEmpty) {
        info("Recovery files found and resuming recovery if possible ({}).", recoveryFiles.map(_.getName).mkString(","))
        resumeTimer.time {
          // Compare the last records from the member log and previous recovery log
          val memberTxLog = new FileTransactionLog(member.serviceName, member.token, logDir, serializer = serializer)
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
                onRecoveryFailure
              }
            }
            case (None, Some(lastRecoveredIndex)) => {
              // No member log, just recovery ones! Someone deleted the member log during or after the previous attempt!
              resumeError.mark()
              error("Previous recovery wasn't complete. There are no member transaction logs, just the partially " +
                "recovered ones. Cannot recover member {}.", member)
              onRecoveryFailure
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
    // Truncate member log from the first recovered request record
    val firstRecoveredRecord = firstRequestRecord(recoveryTxLog).flatMap(r => {
      firstRequestRecord(memberTxLog, Some(r.timestamp))
    })
    firstRecoveredRecord.foreach(memberTxLog.truncate(_))

    // Move recovery log files into the real log directory
    for (recoveryFile <- recoveryTxLog.getLogFiles) {
      Files.move(recoveryFile.toPath, new File(logDir, recoveryFile.getName).toPath)
    }

    // Delete recovery directory (will fail if not empty but this is ok)
    recoveryDir.delete()
  }

  /**
   * Returns a tuple with the transaction of the specified consistent timestamp and the sequence of all the following
   * transactions (complete and incomplete). The consistent timestamp transaction is not in the sequence and the
   * sequence is ordered by timestamp.
   */
  private def loadAllPending(txLog: FileTransactionLog, consistentTimestamp: Option[Timestamp]):
  (Option[PendingTransaction], Seq[PendingTransaction]) = {

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

      // Extract the consistent transaction from the pending transactions
      val consistentTx = consistentTimestamp match {
        case Some(timestamp) => {
          val consistentTx = pending(timestamp)
          pending -= timestamp
          Some(consistentTx)
        }
        case None => None
      }

      (consistentTx, pending.values.toList.sorted)
    } finally {
      it.close()
    }
  }

  /**
   * Returns the first Request record starting at the specified consistent timestamp.
   */
  private def firstRequestRecord(txLog: FileTransactionLog, consistentTimestamp: Option[Timestamp] = None): Option[TimestampedRecord] = {
    val it = consistentTimestamp match {
      case Some(timestamp) => txLog.read(timestamp)
      case None => txLog.read
    }
    try {
      it.collectFirst {
        case request: Request => request
      }
    } finally {
      it.close()
    }
  }

  /**
   * Truncate the specified transaction log starting after the specified timestamp. The truncation is exclusive i.e.
   * done from the Request record immediately following the record matching the specified timestamp. This method
   * assumes that the records from the specified timestamp are ordered by timestamp in ascending order
   * (e.g. rewritten by recovery).
   */
  def truncateAfter(txLog: FileTransactionLog, timestamp: Timestamp) {
    val it = txLog.read(timestamp)
    val nextRequest = try {
      it.collectFirst {
        case request: Request if request.timestamp > timestamp => request
      }
    } finally {
      it.close()
    }

    nextRequest match {
      case Some(request) => txLog.truncate(request)
      case _ =>
    }
  }
}
