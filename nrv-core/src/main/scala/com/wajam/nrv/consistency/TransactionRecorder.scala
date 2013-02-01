package com.wajam.nrv.consistency

import actors.Actor
import com.yammer.metrics.scala.Instrumented
import com.wajam.nrv.service.{ServiceMember, Service}
import persistence.{TransactionEvent, FileTransactionLog}
import com.wajam.nrv.data.Message
import com.wajam.nrv.utils.timestamp.Timestamp
import com.wajam.nrv.utils.Scheduler
import util.Random
import com.wajam.nrv.Logging

class TransactionRecorder(val service: Service, val member: ServiceMember, logDir: String) extends Instrumented {

  val txLog: FileTransactionLog = new FileTransactionLog(service.name, member.token, logDir, validateTimestamp = false)
  val commitScheduler = new Scheduler(AppenderActor, Commit, Random.nextInt(5000), 5000, blockingMessage = true, autoStart = false)

  def start() {
    AppenderActor.start()
    commitScheduler.start()
  }

  def append(message: Message) {
    // TODO: Properly lookup message timestamp
    val timestamp = Timestamp(System.currentTimeMillis() * 1000)
    AppenderActor ! Append(TransactionEvent(timestamp, None, message.token, message))
  }

  def kill() {
    commitScheduler.cancel()
    AppenderActor ! Kill
  }

  def appendQueueSize = AppenderActor.queueSize

  private case class Append(tx: TransactionEvent)

  private object Commit

  private object Kill

  private object AppenderActor extends Actor with Logging {

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
              info("Commit transaction log: {}", txLog)
              txLog.commit()
            } catch {
              case e: Exception => {
              }
            } finally {
              sender ! true
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
