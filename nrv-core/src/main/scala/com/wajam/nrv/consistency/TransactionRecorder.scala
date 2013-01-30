package com.wajam.nrv.consistency

import actors.Actor
import com.yammer.metrics.scala.Instrumented
import com.wajam.nrv.service.{Service, TokenRange}
import persistence.{TransactionEvent, FileTransactionLog}
import com.wajam.nrv.data.Message
import com.wajam.nrv.utils.timestamp.Timestamp

class TransactionRecorder(val service: Service, val range: TokenRange, logDir: String) extends Instrumented {

  val txLog: FileTransactionLog = new FileTransactionLog(service.name, range.end, logDir, validateTimestamp = false)

  def start() {
    AppenderActor.start()
  }

  def append(message: Message) {
    val timestamp = message.metadata("timestamp").asInstanceOf[Timestamp]
    AppenderActor ! TransactionEvent(timestamp, None, message.token, message)
  }

  def commit() {
    AppenderActor ! Commit
  }

  def kill() {
    AppenderActor ! Kill
  }

  def appendQueueSize = AppenderActor.queueSize

  private case class Append(tx: TransactionEvent)

  private object Commit

  private object Kill

  private object AppenderActor extends Actor {

    def queueSize = mailboxSize

    def act() {
      loop {
        react {
          case Append(tx) => {
            try {
              txLog.append(tx)
            } catch {
              case e: Exception => {
              }
            }
          }
          case Commit => {
            try {
              txLog.commit()
            } catch {
              case e: Exception => {
              }
            }
          }
          case Kill => {
            try {
              txLog.commit()
              txLog.close()
              exit()
            } catch {
              case e: Exception => {
              }
            }
          }
        }
      }
    }
  }

}
