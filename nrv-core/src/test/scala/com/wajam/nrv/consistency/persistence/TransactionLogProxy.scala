package com.wajam.nrv.consistency.persistence

import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._
import com.wajam.nrv.consistency.persistence.LogRecord.Index
import com.wajam.nrv.utils.timestamp.Timestamp
import org.mockito.Mockito

trait TransactionAppender {
  def append(record: LogRecord)
}

/**
 * TransactionLog cannot be mock directly because the #append method takes a function argument.
 */
class TransactionLogProxy(ignoreGetLastLoggedRecord: Boolean = true, ignoreCommit: Boolean = true)
  extends TransactionLog with MockitoSugar {

  val mockAppender = mock[TransactionAppender]
  val mockTxLog = mock[TransactionLog]
  when(mockTxLog.getLastLoggedRecord).thenReturn(None)

  def getLastLoggedRecord = mockTxLog.getLastLoggedRecord

  def append[T <: LogRecord](block: => T): T = {
    val record: T = block
    mockAppender.append(record)
    record
  }

  def read(index: Index) = mockTxLog.read(index)

  def read(timestamp: Timestamp) = mockTxLog.read(timestamp)

  def truncate(index: Index) {
    mockTxLog.truncate(index)
  }

  def commit() {
    mockTxLog.commit()
  }

  def close() {
    mockTxLog.commit()
  }

  def verifyZeroInteractions() {
    applyIgnoreRules()
    Mockito.verifyZeroInteractions(mockAppender, mockTxLog)
  }

  def verifyNoMoreInteractions() {
    applyIgnoreRules()
    Mockito.verifyNoMoreInteractions(mockAppender, mockTxLog)
  }

  def applyIgnoreRules() {
    if (ignoreGetLastLoggedRecord) {
      verify(mockTxLog, atLeast(0)).getLastLoggedRecord
    }
    if (ignoreCommit) {
      verify(mockTxLog, atLeast(0)).commit()
    }
  }
}
